FROM alpine:3.20

ARG VERSION
ARG GIT_SHA

LABEL version="${VERSION}"
LABEL git-sha="${GIT_SHA}"
LABEL description="This is a Docker image for the BatControl project."
LABEL maintainer="matthias.strubel@aod-rpg.de"

ENV BATCONTROL_VERSION=${VERSION}
ENV BATCONTROL_GIT_SHA=${GIT_SHA}
# Set default timezone to UTC, override with -e TZ=Europe/Berlin or similar 
# when starting the container 
# or set the timezone in docker-compose.yml in the environment section,
ENV TZ=UTC  

RUN mkdir -p /app /app/logs /app/config
WORKDIR /app
RUN apk add --no-cache \
            python3 \
            py3-numpy \
            py3-pandas\
            py3-yaml\
            py3-requests\
            py3-paho-mqtt \
            tzdata


COPY *.py ./
COPY LICENSE ./
COPY config/load_profile_default.csv ./config/load_profile.csv
COPY config/load_profile_default.csv ./default_load_profile.csv
COPY config ./config_template
COPY dynamictariff ./dynamictariff
COPY inverter ./inverter
COPY forecastconsumption ./forecastconsumption
COPY forecastsolar ./forecastsolar
COPY logfilelimiter ./logfilelimiter
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

VOLUME [ "/app/logs" , "/app/config" ]

CMD [ "/bin/sh", "/app/entrypoint.sh" ]
