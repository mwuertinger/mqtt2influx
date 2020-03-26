# mqtt2influx
Subscribes to sensor data from [sensorbox](https://github.com/mwuertinger/sensorbox)
and forwards it to InfluxDB.
```
sensorbox -> MQTT Broker -> mqtt2influx -> InfluxDB
```