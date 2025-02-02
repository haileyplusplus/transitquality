# https://github.com/motis-project/motis/blob/860da4d455e2bbb50186e5e0625ba611cb329a52/Dockerfile

FROM ghcr.io/motis-project/motis:2.0.9

USER root

RUN apk --no-cache add curl

EXPOSE 7001
#RUN addgroup -S motis && adduser -S motis -G motis && \
#    mkdir /data && \
#    chown motis:motis /data
#EXPOSE 8080
VOLUME ["/data"]

RUN chown motis /data

USER motis
RUN mkdir /data/rundir
COPY motis-config.yaml /data/rundir/config.yml

WORKDIR /data

RUN curl -O https://download.bbbike.org/osm/bbbike/Chicago/Chicago.osm.pbf
RUN curl -O https://www.transitchicago.com/downloads/sch_data/google_transit.zip

WORKDIR /motis

#RUN /motis import -d /data -c /data/config.yml
CMD ["/motis", "server", "-d", "/data/rundir"]
#CMD ["tail", "-f", "/dev/null"]
