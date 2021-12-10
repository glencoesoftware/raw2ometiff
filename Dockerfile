
FROM debian:10 as build

ARG release=0.3.0

USER root
WORKDIR /tmp
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -q -y && apt-get install -y -q \
      unzip wget
RUN wget -q -O the_archive.zip "https://github.com/glencoesoftware/raw2ometiff/releases/download/v${release}/raw2ometiff-${release}.zip"
RUN unzip the_archive.zip
RUN cd raw2ometiff-* \
 && mkdir -p /tmp/staging-area \
 && mv * /tmp/staging-area


FROM openjdk:8 as final

USER root
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update -y -q \
 && apt-get install -y -q libblosc1 \
 && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=build /tmp/staging-area/ /opt/raw2ometiff/
ENV PATH="/opt/raw2ometiff/bin:${PATH}"

USER 1000
WORKDIR /opt/raw2ometiff
CMD ["raw2ometiff"]
