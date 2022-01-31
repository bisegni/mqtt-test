package mqtt_exec_rmq

import "testing"

func TestMQQTTest(t *testing.T) {
	var config = &TestConfig{
		Broker:               "amqp://admin:admin@LiwaxM1.local:5672/",
		Topic:                "test",
		Qos:                  "0",
		InstanceNumber:       1,
		IterationForInstance: 100000,
		SamplePacketNumber:   1000,
	}

	ExecuteTest(config)
}
