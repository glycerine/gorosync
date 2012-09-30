package cpg

/*
#cgo LDFLAGS: -lcpg -lcorosync_common

#include <corosync/cpg.h>
*/
import "C"

func cpg_iter_init(handle C.cpg_handle_t, itype C.cpg_iteration_type_t, group *C.struct_cpg_name) (C.cpg_iteration_handle_t, C.cs_error_t) {
	var ret C.cpg_iteration_handle_t

	err := C.cpg_iteration_initialize(handle, itype, group, &ret)
	if err != C.CS_OK {
		return 0, err
	}

	return ret, C.CS_OK
}

func cpg_iter_finalize(ihandle C.cpg_iteration_handle_t) {
	C.cpg_iteration_finalize(ihandle)
}

func cpg_iter_next(ihandle C.cpg_iteration_handle_t) (string, Process, C.cs_error_t) {
	var ret C.struct_cpg_iteration_description_t

	err := C.cpg_iteration_next(ihandle, &ret)
	if err != C.CS_OK {
		return "", Process{}, err
	}

	return C.GoString(&ret.group.value[0]), Process{uint32(ret.nodeid), uint32(ret.pid)}, C.CS_OK
}

func doIter(ih C.cpg_iteration_handle_t, fn func(string, Process), done func()) {
	defer cpg_iter_finalize(ih)
	defer done()

	for {
		group, proc, ret := cpg_iter_next(ih)
		if ret != C.CS_OK {
			break
		}

		fn(group, proc)
	}
}

func (c *Cpg) GetProcesses() (<-chan Process, error) {
	cn := cpgname(*c.group)

	ihandle, ret := cpg_iter_init(c.handle, C.CPG_ITERATION_ONE_GROUP, cn)
	err := makeErr("cpg_iter_init", ret)
	if err != nil {
		return nil, err
	}

	ch := make(chan Process)

	go doIter(ihandle,
		func(_ string, proc Process) { ch <- proc },
		func() { close(ch) })

	return ch, nil
}

type GroupProc struct {
	Group string
	Proc  Process
}

func GetAllProcesses() (<-chan GroupProc, error) {
	handle, err := cpg_model_init()
	if err != nil {
		return nil, err
	}
	defer cpg_model_finalize(handle)

	ihandle, ret := cpg_iter_init(handle, C.CPG_ITERATION_ALL, nil)
	err = makeErr("cpg_iter_init", ret)
	if err != nil {
		return nil, err
	}

	ch := make(chan GroupProc)


	go doIter(ihandle,
		func (group string, proc Process) { ch <- GroupProc{group, proc} },
		func () { close(ch) })

	return ch, nil
}

func GetGroups() (<-chan string, error) {
	handle, err := cpg_model_init()
	if err != nil {
		return nil, err
	}
	defer cpg_model_finalize(handle)

	ihandle, ret := cpg_iter_init(handle, C.CPG_ITERATION_NAME_ONLY, nil)
	err = makeErr("cpg_iter_init", ret)
	if err != nil {
		return nil, err
	}

	ch := make(chan string)

	go doIter(ihandle,
		func(group string, _ Process) { ch <- group },
		func() { close(ch) })

	return ch, nil
}
