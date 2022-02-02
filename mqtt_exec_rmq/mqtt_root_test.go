package mqtt_exec_rmq

import "testing"

func TestMQQTTest(t *testing.T) {
	var config = &TestConfig{
		Broker:               "rabbitmq-stream://guest:guest@localhost:5552/",
		Topic:                "test",
		Qos:                  100,
		InstanceNumber:       1,
		IterationForInstance: 1000,
		SamplePacketNumber:   1024,
		RaisedTo:             20,
	}

	ExecuteTest(config)
}
