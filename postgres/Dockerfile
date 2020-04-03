FROM postgres:11.5

ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD simple
ENV POSTGRES_DB venmo_challenge

COPY structure.sql /docker-entrypoint-initdb.d/001_structure.sql

