package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/yosssi/gmq/mqtt/client"
	"gopkg.in/yaml.v3"
)

var config *Config
var influxClient influxdb.Client

func main() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s <config>", os.Args[0])
	}
	var err error
	config, err = parseConfig(os.Args[1])
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
		Handler:     mqttHandler,
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
	Location    string
	ClockDrift  int64
	Uptime      int64
	Pressure    float64
	Humidity    float64
	Temperature float64
	CO2         int
}

// mqttHandler called for every MQTT message
func mqttHandler(topic, message []byte) {
	m, err := parseMessage(message)
	if err != nil {
		log.Printf("parseMessage: %v", err)
		return
	}
	log.Printf("%+v", m)
	if err := writeToInflux(m); err != nil {
		log.Printf("writeToInflux: %v", err)
	}
}

// parseMessage parses a CSV message from a sensorbox
func parseMessage(message []byte) (*measurements, error) {
	var m measurements
	tokens := strings.Split(string(message), ",")
	if len(tokens) < 6 {
		return nil, fmt.Errorf("mqttHandler: not enough fields: %s", string(message))
	}

	devId, err := strconv.Atoi(tokens[0])
	if err != nil {
		return nil, fmt.Errorf("devId: %w", err)
	}
	dev, ok := config.Devices[devId]
	if !ok {
		return nil, fmt.Errorf("unknown device: %d", devId)
	}
	m.Location = dev.Location

	t, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("time: %w", err)
	}
	m.ClockDrift = time.Now().Unix() - t

	m.Uptime, err = strconv.ParseInt(tokens[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("uptime: %w", err)
	}

	m.Pressure, err = strconv.ParseFloat(tokens[3], 64)
	if err != nil {
		return nil, fmt.Errorf("pressur: %v", err)
	}

	m.Humidity, err = strconv.ParseFloat(tokens[4], 64)
	if err != nil {
		return nil, fmt.Errorf("humidity: %v", err)
	}

	m.Temperature, err = strconv.ParseFloat(tokens[5], 64)
	if err != nil {
		return nil, fmt.Errorf("temperature: %v", err)
	}

	m.CO2, err = strconv.Atoi(tokens[6])
	if err != nil {
		return nil, fmt.Errorf("co2: %v", err)
	}

	return &m, nil
}

// writeToInflux writes measurements to InfluxDB
func writeToInflux(m *measurements) error {
	// Create a new point batch
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  "sensors",
		Precision: "s",
	})
	if err != nil {
		return err
	}
	tags := map[string]string{"location": m.Location}
	fields := map[string]interface{}{}
	fields["clockdrift"] = m.ClockDrift
	if m.Pressure > 0 {
		fields["pressure"] = m.Pressure
	}
	if m.Uptime > 0 {
		fields["uptime"] = m.Uptime
	}
	if m.Humidity > 0 {
		fields["humidity"] = m.Humidity
	}
	if m.Temperature > 0 {
		fields["temperature"] = m.Temperature - 273.15 // convert to °C
	}
	if m.CO2 > 0 {
		fields["co2"] = m.CO2
	}

	log.Printf("writing to influx: %+v", fields)

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
	Mqtt    MqttConfig     `yaml:"mqtt"`
	Influx  InfluxConfig   `yaml:"influx"`
	Devices map[int]Device `yaml:"devices"`
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

type Device struct {
	Location string `yaml:"location"`
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
