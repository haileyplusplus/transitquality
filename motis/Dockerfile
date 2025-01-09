FROM ghcr.io/motis-project/motis:2.0.9

RUN mkdir /data


COPY motis-config.yaml /data/config.yml
EXPOSE 7001
RUN motis import
CMD motis server -d /data
