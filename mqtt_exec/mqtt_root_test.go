package mqtt_exec

import "testing"

func TestMQQTTest(t *testing.T) {
	var config = &TestConfig{
		Broker:               "tcp://localhost:1883/mqtt",
		Topic:                "test",
		Qos:                  "0",
		InstanceNumber:       1,
		IterationForInstance: 10000,
		SamplePacketNumber:   1000,
	}

	ExecuteTest(config)
}
