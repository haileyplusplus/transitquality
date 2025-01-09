# https://github.com/motis-project/motis/blob/860da4d455e2bbb50186e5e0625ba611cb329a52/Dockerfile

#FROM ghcr.io/motis-project/motis:2.0.9
FROM alpine:latest

COPY motis-config.yaml /data/config.yml
EXPOSE 7001
RUN addgroup -S motis && adduser -S motis -G motis && \
    chown motis:motis /data

RUN apk add cmake
RUN apk add clang18
RUN apk add ninja
RUN apk add git
RUN apk add g++

# build-base llvm-static llvm-dev clang-static clang-dev
RUN apk add build-base llvm18-static llvm18-dev clang18-static clang18-dev

VOLUME ["/data"]

RUN git clone https://github.com/motis-project/motis.git

#USER motis
#RUN /motis import
#CMD ["/motis", "server", "/data"]
CMD ["tail", "-f", "/dev/null"]
