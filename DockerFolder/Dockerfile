# syntax=docker/dockerfile:1
FROM postgis/postgis:15-3.4

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		postgresql-15-pgrouting \
	&& rm -rf /var/lib/apt/lists/*

COPY db/schema.sql /docker-entrypoint-initdb.d/00-schema.sql
