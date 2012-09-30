#include "_cgo_export.h"

static cpg_model_v1_data_t cpg_model_v1 = {
	.model = CPG_MODEL_V1,
	.cpg_deliver_fn = (cpg_deliver_fn_t)go_deliver_fn,
	.cpg_confchg_fn = (cpg_confchg_fn_t)go_confchg_fn,
	.cpg_totem_confchg_fn = (cpg_totem_confchg_fn_t)go_totem_confchg_fn,
	.flags = CPG_MODEL_V1_DELIVER_INITIAL_TOTEM_CONF,
};
cpg_model_data_t *cpg_model = (cpg_model_data_t *)&cpg_model_v1;
