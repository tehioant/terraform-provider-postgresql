##
## Pull latest pg_image
##
FROM postgres:${PGVERSION:-latest}
WORKDIR /go/src

##
## Install anon
##
RUN apt-get update
RUN set -eux &&  \
    apt-get update && \
    apt-get install -y --no-install-recommends  \
    make gcc  \
    postgresql postgresql-server-dev-15  \
    pgxnclient

RUN pgxn install postgresql_anonymizer

#RUN psql postgres -c wal_level=logical
#RUN psql postgres -c max_replication_slots=10
#RUN psql postgres -c session_preload_libraries = 'anon'

RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./anon/anon-init.sql /docker-entrypoint-initdb.d/anon-init.sql