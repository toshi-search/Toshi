FROM rust:1.30-stretch

USER root
RUN apt-get update -y
RUN apt-get -y install capnproto