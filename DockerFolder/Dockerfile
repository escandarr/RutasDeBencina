# syntax=docker/dockerfile:1
FROM postgres:15-alpine

RUN apk add --no-cache bash

COPY db/schema.sql /docker-entrypoint-initdb.d/00-schema.sql
