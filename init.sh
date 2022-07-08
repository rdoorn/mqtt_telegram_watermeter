#!/bin/bash

case $1 in
    start)
        docker run --name=%NAME% \
        --net=domoticanet \
        --restart=unless-stopped \
        -d \
        -e TZ=Europe/Amsterdam \
        -e NEST_API_KEY=""
        -e MQTT_URL="mqtt://mosquitto:1883/"
        %NAME%
        ;;
    stop)
        docker stop %NAME% | xargs docker rm
        ;;
    *)
        echo "unknown or missing parameter $1"
esac
