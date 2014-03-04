PROJECT = repdis
ERL_LIBS = /Users/jay/Git/proper

CT_OPTS = -cover test/repdis.coverspec
CT_SUITES = repdis_pd

include erlang.mk
