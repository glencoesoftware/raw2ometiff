
ARG BUILD_IMAGE=gradle:6.9-jdk8

FROM ${BUILD_IMAGE} as build

USER root
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libblosc1
RUN mkdir -p /raw2ometiff \
 && chown 1000:1000 /raw2ometiff

# Build all
USER 1000
COPY --chown=1000:1000 . /raw2ometiff
WORKDIR /raw2ometiff
RUN gradle build
RUN cd build/distributions && rm raw2ometiff*tar && unzip raw2ometiff*zip && rm -rf raw2ometiff*zip


FROM openjdk:8 as final

USER root
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update -y -q \
 && apt-get install -y --no-install-recommends -q libblosc1 \
 && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=build /raw2ometiff/build/distributions/raw2ometiff* /opt/raw2ometiff
ENV PATH="/opt/raw2ometiff/bin:${PATH}"

USER 1000
WORKDIR /opt/raw2ometiff
ENTRYPOINT ["/opt/raw2ometiff/bin/raw2ometiff"]
