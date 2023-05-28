#!/bin/bash 

export $(grep -v '^#' .env | xargs)

# create database
PGPASSWORD=$SINK_PSQL_PASS createdb -h $SINK_PSQL_HOST -p $SINK_PSQL_PORT -U $SINK_PSQL_USER $SINK_PSQL_DB

# create table from sql/ddl.sql
PGPASSWORD=$SINK_PSQL_PASS psql -h $SINK_PSQL_HOST -p $SINK_PSQL_PORT -U $SINK_PSQL_USER < sql/ddl.sql