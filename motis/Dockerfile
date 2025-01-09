FROM ghcr.io/motis-project/motis:2.0.9

ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    motisuser

RUN mkdir /data
RUN chown motisuser /data

USER motisuser

COPY motis-config.yaml /data/config.yml

EXPOSE 7001

RUN motis import

CMD motis server -d /data
