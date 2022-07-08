package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rdoorn/gohelper/mqtthelper"
	"github.com/rdoorn/gohelper/statsdhelper"
)

const mqttClientID = "mqtt_telegram_watermeter"

type Handler struct {
	mqtt   *mqtthelper.Handler
	statsd *statsdhelper.Handler
}

func (h *Handler) mqttOut(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("* [%s] %s\n", msg.Topic(), string(msg.Payload()))

	switch msg.Topic() {
	case "watermeter/reading/current_value":

		log.Printf("watermeter.current_value=%s", msg.Payload())
		err := h.statsd.Gauge(1.0, fmt.Sprintf("watermeter.current_value", fmt.Sprintf("%s", msg.Payload())))
		if err != nil {
			log.Printf("err: %s", err)
		}
	}
}

func main() {

	h := Handler{
		mqtt:   mqtthelper.New(),
		statsd: statsdhelper.New(),
	}

	// Setup MQTT Sub
	err := h.mqtt.Subscribe(mqttClientID, "watermeter/reading/current_value", 0, h.mqttOut)
	if err != nil {
		panic(err)
	}

	// loop till exit
	sigterm := make(chan os.Signal, 10)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-sigterm:
			log.Printf("Program killed by signal!")
			return
		}
	}
}
