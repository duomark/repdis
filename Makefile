PROJECT = repdis

## Extract the first digit of the emulator version from:
## Erlang (SMP,ASYNC_THREADS,HIPE) (BEAM) emulator version 6.1
ERL_CMD = erl +V 2>&1 | grep -oh "version .*" | cut -f 2 -d ' ' | cut -c 1
ERL_VERSION := $(shell ${ERL_CMD})

ifeq "$(ERL_VERSION)" "6"
	ADDL_FLAGS = "-DSUPPORTS_DICT_TYPE_PREFIX"
endif

ERLC_OPTS += ${ADDL_FLAGS} -Werror +debug_info +warn_export_all +warn_export_vars \
               +warn_shadow_vars +warn_obsolete_guard # +bin_opt_info +warn_missing_spec

TEST_DEPS = proper
dep_proper = pkg://proper

CT_OPTS = -cover test/repdis.coverspec
CT_SUITES = repdis_pd

include erlang.mk
