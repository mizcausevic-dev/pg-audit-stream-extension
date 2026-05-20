EXTENSION   = audit_stream
EXTVERSION  = 0.1.0
DATA        = audit_stream--$(EXTVERSION).sql
PG_CONFIG  ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
