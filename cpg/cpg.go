package cpg

/*
#cgo LDFLAGS: -lcpg -lcorosync_common

#include <string.h>
#include <stdlib.h>
#include <poll.h>
#include <corosync/cpg.h>
#include <corosync/corotypes.h>

extern cpg_model_data_t *cpg_model;
*/
import "C"

import (
	"fmt"
	"github.com/badgerodon/collections/set"
	"os"
	"reflect"
	"runtime"
	"unsafe"
)

const (
	ZCB_SIZE = 1024 * 1024
)

type Cpg struct {
	handle C.cpg_handle_t
	group  *string

	self Process

	tx chan []byte      // app to corosync
	rx chan interface{} // corosync to app
}

type Process struct {
	Nodeid uint32
	Pid    uint32
}

type Data struct {
	Sender Process
	Data   []byte
}

func (d *Data) Read() ([]byte, error) {
	return d.Data, nil
}

// Channel type used for signalling termination
type ctlCh chan struct{}

type ConfChg struct {
	members, left, joined *set.Set
}

type Txbytechan chan<- []byte

func (ch Txbytechan) Write(data []byte) (int, error) {
	ch <- data
	return len(data), nil
}

type Error struct {
	msg   string
	cserr C.cs_error_t
}

func (e Error) Error() string {
	return e.msg + " failed: " + C.GoString(C.cs_strerror(e.cserr))
}

func makeErr(msg string, err C.cs_error_t) error {
	if err != C.CS_OK {
		return Error{msg, err}
	}
	return nil
}

func cpgname(name string) *C.struct_cpg_name {
	if len(name) >= C.CPG_MAX_NAME_LENGTH {
		return nil
	}

	var cn C.struct_cpg_name

	cs := []byte(name)

	cn.length = C.uint32_t(len(cs))
	C.strncpy(&cn.value[0], (*C.char)((unsafe.Pointer)(&cs[0])), C.size_t(unsafe.Sizeof(cn.value)))

	return &cn
}

func (cpg *Cpg) txloop(errch chan<- error) {
	defer close(errch)

	var buffer unsafe.Pointer

	err := makeErr("cpg_zcb_alloc", C.cpg_zcb_alloc(cpg.handle, ZCB_SIZE, &buffer))

	if err != nil {
		errch <- err
		return
	}
	defer C.cpg_zcb_free(cpg.handle, buffer)

	var bufslice []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bufslice))
	hdr.Cap = ZCB_SIZE
	hdr.Len = ZCB_SIZE
	hdr.Data = uintptr(buffer)

	for msg := range cpg.tx {
		if len(msg) > ZCB_SIZE {
			errch <- makeErr("msg too long", C.CS_ERR_TOO_BIG)
			continue
		}

		sz := copy(bufslice, msg)

		err = makeErr("cpg_zcb_mcast_joined",
			C.cpg_zcb_mcast_joined(cpg.handle, C.CPG_TYPE_AGREED,
				buffer, C.size_t(sz)))
		if err != nil {
			errch <- err
			break
		}
	}
}

func getchan(handle C.cpg_handle_t) chan interface{} {
	var ptr unsafe.Pointer
	err := makeErr("cpg_context_get", C.cpg_context_get(handle, &ptr))
	if err != nil {
		panic(err)
	}

	return *(*chan interface{})(ptr)
}

//export go_deliver_fn
func go_deliver_fn(handle C.cpg_handle_t, name *C.struct_cpg_name,
	nodeid C.uint32_t, pid C.uint32_t, data unsafe.Pointer, len C.size_t) {

	ch := getchan(handle)

	ch <- &Data{Process{uint32(nodeid), uint32(pid)}, C.GoBytes(data, C.int(len))}
}

func cvtprocs(list *C.struct_cpg_address, entries C.size_t) *set.Set {
	var slice []C.struct_cpg_address
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	hdr.Cap = int(entries)
	hdr.Len = int(entries)
	hdr.Data = uintptr(unsafe.Pointer(list))

	set := set.New()

	for i := 0; i < int(entries); i++ {
		set.Insert(Process{uint32(slice[i].nodeid), uint32(slice[i].pid)})
	}

	return set
}

//export go_confchg_fn
func go_confchg_fn(handle C.cpg_handle_t, name *C.struct_cpg_name,
	member_list *C.struct_cpg_address, member_list_entries C.size_t,
	left_list *C.struct_cpg_address, left_list_entries C.size_t,
	joined_list *C.struct_cpg_address, joined_list_entries C.size_t) {

	ch := getchan(handle)

	ch <- &ConfChg{
		members: cvtprocs(member_list, member_list_entries),
		left:    cvtprocs(left_list, left_list_entries),
		joined:  cvtprocs(joined_list, joined_list_entries),
	}
}

//export go_totem_confchg_fn
func go_totem_confchg_fn(handle C.cpg_handle_t, ring_id C.struct_cpg_ring_id,
	member_list_entries C.uint32_t, member_list *C.uint32_t) {
	// nothing for now
}

func (cpg *Cpg) cpgloop(ctlch ctlCh) {
	defer func() { close(cpg.rx); cpg.rx = nil }()

	C.cpg_context_set(cpg.handle, unsafe.Pointer(&cpg.rx))

	// Recover from panics in callbacks
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("recovering from panic", err)
			cpg.rx <- err
		}
	}()

	var cpgfd C.int

	err := makeErr("cpg_fd_get", C.cpg_fd_get(cpg.handle, &cpgfd))
	if err != nil {
		cpg.rx <- err
		return
	}

	// Use ctlch to close down poll goroutine too
	pollch := poll(map[int]uint { int(cpgfd): PollIn }, ctlch)

	for {
		select {
		case _, ok := <-ctlch:
			// If ctlch closes, then it's time for us to exit
			if  !ok {
				return
			}

		case <- pollch:
			err := makeErr("cpg_dispatch",
				C.cpg_dispatch(cpg.handle, C.CS_DISPATCH_ALL))
			if err != nil {
				cpg.rx <- err
				return
			}
		}
	}
}

func (cpg *Cpg) rxloop(errch <-chan error) {
	ctlch := make(ctlCh)
	defer close(ctlch)

	go cpg.cpgloop(ctlch)

	// Relay tx errors through; if errch closes, we need to shut
	// up shop too
	for e := range errch {
		cpg.rx <- e
	}
}

func (cpg *Cpg) Self() Process {
	var nodeid C.uint

	err := makeErr("cpg_local_get", C.cpg_local_get(cpg.handle, &nodeid))
	if err != nil {
		panic(err)
	}

	return Process{uint32(nodeid), uint32(os.Getpid())}
}

func (cpg *Cpg) Tx() Txbytechan {
	return cpg.tx
}

func (cpg *Cpg) Rx() <-chan interface{} {
	return cpg.rx
}

func (cpg *Cpg) waitself() error {
	for msg := range cpg.rx {
		switch val := msg.(type) {
		case error:
			fmt.Println("Got error")
			return val

		case *ConfChg:
			if val.members.Has(cpg.Self()) {
				go func() { cpg.rx <- val }()
				return nil
			}
		}
	}
	return nil
}

func cpg_model_init() (C.cpg_handle_t, error) {
	var handle C.cpg_handle_t

	err := makeErr("cpg_model_initialize",
		C.cpg_model_initialize(&handle, C.CPG_MODEL_V1, C.cpg_model, nil))

	return handle, err
}

func cpg_model_finalize(handle C.cpg_handle_t) {
	C.cpg_finalize(handle)
}

func Join(group string) (*Cpg, error) {
	cpg := &Cpg{}
	runtime.SetFinalizer(cpg, func(cpg *Cpg) { cpg.Leave() })

	handle, err := cpg_model_init()
	if err != nil {
		return nil, err
	}
	cpg.handle = handle

	cn := cpgname(group)

	err = makeErr("cpg_join", C.cpg_join(cpg.handle, cn))
	if err != nil {
		return nil, err
	}

	cpg.group = &group

	cpg.tx = make(chan []byte, 1)
	cpg.rx = make(chan interface{}, 1)

	// Funnel tx errors through the rx channel for user
	// consumption; also allows everything to mop up
	errch := make(chan error, 1)

	go cpg.txloop(errch)
	go cpg.rxloop(errch)

	if err = cpg.waitself(); err != nil {
		return nil, err
	}

	return cpg, nil
}

func (cpg *Cpg) Leave() {
	if cpg.group != nil {
		cn := cpgname(*cpg.group)

		cpg.group = nil

		C.cpg_leave(cpg.handle, cn)
	}

	if cpg.tx != nil {
		// Close transmit side
		close(cpg.tx)
		cpg.tx = nil

		// Wait for rx side to shut down
		for _ = range cpg.rx {
		}
	}

	if cpg.handle != 0 {
		cpg_model_finalize(cpg.handle)
		cpg.handle = 0
	}
}
