FROM apache/iceberg-rest-fixture:1.10.1

USER root
RUN curl -fsSL -o /usr/lib/iceberg-rest/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.5.jar \
    && chown iceberg:iceberg /usr/lib/iceberg-rest/postgresql.jar
USER iceberg
