package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/yosssi/gmq/mqtt/client"
	"gopkg.in/yaml.v3"
)

var influxClient influxdb.Client

func main() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) != 2 {
		log.Fatal("Usage: %s <config>", os.Args[0])
	}
	config, err := parseConfig(os.Args[1])
	if err != nil {
		log.Fatalf("loading config failed: %v", err)
	}

	influxClient, err = influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: config.Influx.Server,
	})
	if err != nil {
		log.Println("Error creating InfluxDB Client: ", err.Error())
		return
	}

	mqttClient, err := newMqttClient(config)
	if err != nil {
		log.Fatalf("creating mqtt client failed: %v", err)
	}
	err = mqttClient.Subscribe(&client.SubscribeOptions{SubReqs: []*client.SubReq{{
		TopicFilter: []byte("sensorbox/measurements"),
		Handler: mqttHandler,
	}}})
	if err != nil {
		log.Fatalf("mqtt subscribe failed: %v", err)
	}

	<-sigc
	if err = mqttClient.Disconnect(); err != nil {
		log.Printf("MQTT disconnect: %v", err)
	}
	if err := influxClient.Close(); err != nil {
		log.Printf("Influx close: %v", err)
	}
}

// measurements is used to parse the sensorbox data
type measurements struct {
	Client      string  `json:"C"`
	Time        int64   `json:"T"`
	Pressure    float64 `json:"p"`
	Humidity    float64 `json:"h"`
	Temperature float64 `json:"t"`
	CO2         int32   `json:"c"`
}

// mqttHandler called for every MQTT message
func mqttHandler(topic, message []byte) {
	var m measurements
	if err := json.Unmarshal(message, &m); err != nil {
		log.Printf("unmarshal: %v", err)
		return
	}
	writeToInflux(m)
}

// writeToInflux writes measurements to InfluxDB
func writeToInflux(m measurements) error {
	// calculate sensorbox clockdrift
	cd := time.Now().Unix() - m.Time

	// Create a new point batch
	bp, _ := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  "sensors",
		Precision: "s",
	})
	tags := map[string]string{"client": m.Client}
	fields := map[string]interface{}{"pressure": m.Pressure, "humidity": m.Humidity, "temperature": m.Temperature, "co2": m.CO2, "clockdrift": cd}
	pt, err := influxdb.NewPoint("measurements", tags, fields, time.Now())
	if err != nil {
		return err
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := influxClient.Write(bp); err != nil {
		return err
	}
	return nil
}

// newMqttClient create MQTT client
func newMqttClient(config *Config) (*client.Client, error) {
	caCert, err := ioutil.ReadFile(config.Mqtt.CaPath)
	if err != nil {
		return nil, fmt.Errorf("unable to load CA: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	connectOpts := client.ConnectOptions{
		Network: "tcp",
		Address: config.Mqtt.Server,
		TLSConfig: &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true, // FIXME
		},
		ClientID: []byte(config.Mqtt.User),
		UserName: []byte(config.Mqtt.User),
		Password: []byte(config.Mqtt.Passwd),
	}

	var mqttClient *client.Client
	mqttClient = client.New(&client.Options{
		ErrorHandler: func(err error) {
			log.Printf("MQTT error: %v", err)
			if err == io.EOF {
				log.Print("Trying to reconnect...")
				err = mqttClient.Connect(&connectOpts)
				if err != nil {
					log.Fatalf("reconnect failed: %v", err)
				}
			}
		},
	})
	err = mqttClient.Connect(&connectOpts)
	if err != nil {
		return nil, fmt.Errorf("MQTT connect: %w", err)
	}
	return mqttClient, nil
}

// Config represents a config file
type Config struct {
	Mqtt   MqttConfig   `yaml:"mqtt"`
	Influx InfluxConfig `yaml:"influx"`
}

type MqttConfig struct {
	Server string `yaml:"server"`
	CaPath string `yaml:"caPath"`
	User   string `yaml:"user"`
	Passwd string `yaml:"passwd"`
}

type InfluxConfig struct {
	Server string `yaml:"server"`
	Token  string `yaml:"token"`
}

// parseConfig reads config file at path and returns the content or an error
func parseConfig(path string) (*Config, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(buf, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
