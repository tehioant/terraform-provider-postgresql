#!/usr/bin/env bash

#apt-get update
#
#apt-get install -y --no-install-recommends \
#  make gcc \
#  postgresql postgresql-server-dev-15 \
#  pgxnclient
#
#pgxn install postgresql_anonymizer
#
## this will emulate the `curl` commands in the how-to
#cp data/* /tmp/
#cp pg_hba.conf /tmp

postgres -c wal_level=logical -c max_replication_slots=10