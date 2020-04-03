FROM apache/nifi:latest

USER root
RUN apt-get update
RUN apt-get install -yq libpostgresql-jdbc-java libpostgis-java libsnappy-java postgresql-client-common

# COPY --chown=nifi:nifi bootstrap.conf /opt/nifi/nifi-current/conf/
COPY --chown=nifi:nifi flow.xml.gz /opt/nifi/nifi-current/conf/
COPY --chown=nifi:nifi postgresql-jdbc4.jar /opt/nifi/nifi-current/lib/
USER nifi
