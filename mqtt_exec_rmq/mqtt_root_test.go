package mqtt_exec_rmq

import "testing"

func TestMQQTTest(t *testing.T) {
	var config = &TestConfig{
		Broker:               "amqp://localhost:5672/",
		Topic:                "test",
		Qos:                  0,
		InstanceNumber:       2,
		IterationForInstance: 10,
		SamplePacketNumber:   1024,
		RaisedTo:             8,
	}

	ExecuteTest(config)
}
