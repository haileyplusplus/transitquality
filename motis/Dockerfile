# https://github.com/motis-project/motis/blob/860da4d455e2bbb50186e5e0625ba611cb329a52/Dockerfile

FROM ghcr.io/motis-project/motis:2.0.9

COPY motis-config.yaml /data/config.yml
EXPOSE 7001
RUN addgroup -S motis && adduser -S motis -G motis && \
    mkdir /data && \
    chown motis:motis /data
EXPOSE 8080
VOLUME ["/data"]
#USER motis
#RUN /motis import
#CMD ["/motis", "server", "/data"]
CMD ["tail", "-f", "/dev/null"]
