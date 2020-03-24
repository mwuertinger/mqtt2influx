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

type Config struct {
	Mqtt   MqttConfig   `yaml:"mqtt"`
	Influx InfluxConfig `yaml:"influx"`
}

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

type measurements struct {
	Client      string  `json:"C"`
	Time        int64   `json:"T"`
	Pressure    float64 `json:"p"`
	Humidity    float64 `json:"h"`
	Temperature float64 `json:"t"`
	CO2         int32   `json:"c"`
}

func main() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	config, err := parseConfig(os.Args[1])
	if err != nil {
		log.Fatalf("loading config failed: %v", err)
	}

	c, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: config.Influx.Server,
	})
	if err != nil {
		log.Println("Error creating InfluxDB Client: ", err.Error())
		return
	}
	defer c.Close()

	mqttClient, err := newMqttClient(config)
	if err != nil {
		log.Fatalf("creating mqtt client failed: %v", err)
	}
	err = mqttClient.Subscribe(&client.SubscribeOptions{SubReqs: []*client.SubReq{{
		TopicFilter: []byte("sensorbox/measurements"),
		Handler: func(topicName, message []byte) {
			log.Printf("MQTT message: %s", string(message))
			var m measurements
			if err := json.Unmarshal(message, &m); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}
			writeToInflux(c, m)
		},
	}}})
	if err != nil {
		log.Fatalf("mqtt subscribe failed: %v", err)
	}

	<-sigc
	err = mqttClient.Disconnect()
	if err != nil {
		log.Printf("MQTT disconnect: %v", err)
	}
}

func writeToInflux(c influxdb.Client, m measurements) {
	// Create a new point batch
	bp, _ := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  "sensors",
		Precision: "s",
	})
	tags := map[string]string{"client": m.Client}
	fields := map[string]interface{}{"pressure": m.Pressure, "humidity": m.Humidity, "temperature": m.Temperature, "co2": m.CO2}
	pt, err := influxdb.NewPoint("measurements", tags, fields, time.Now())
	if err != nil {
		fmt.Println("Error: ", err.Error())
		return
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Printf("write: %v", err)
	}
}

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
