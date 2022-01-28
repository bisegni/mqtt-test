package mqtt_test

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"sync"

	mqtt "github.com/srishina/mqtt.go"
)

var csg sync.WaitGroup
var psg sync.WaitGroup

func consumer(c mqtt.Client, topic string, counter int, qosLevel int64) {
	recvr := mqtt.NewMessageReceiver()

	subscriptions := []*mqtt.Subscription{}
	subscriptions = append(subscriptions, &mqtt.Subscription{TopicFilter: topic, QoSLevel: byte(qosLevel)})

	suback, err := c.Subscribe(context.Background(), subscriptions, nil, recvr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("SUBACK-%d: %v\n", counter, suback)
	//signal wait group
	csg.Done()

	for {
		p, err := recvr.Recv()
		if err != nil {
			return
		}
		log.Printf("CONSUMER %d recvd - Topic: %s QoS: %d Payload: %v\n", counter, p.TopicName, p.QoSLevel, string(p.Payload))
	}
}

func producer(c mqtt.Client, topic string, counter int, qosLevel int64) {
	var payload = "test"
	for i := 10; i < 10; i++ {
		payload = fmt.Sprintf("producer-%d-message-%d", counter, i)
		err := c.Publish(context.Background(), topic, byte(qosLevel), false, []byte(payload), nil)
		if err != nil {
			log.Fatal(err)
		}
	}
	psg.Done()
}

// ExecuteTest ...
func ExecuteTest(broker string, topic string, qos string) {
	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	qosLevel, err := strconv.ParseInt(qos, 10, 64)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	var conn mqtt.Connection
	switch u.Scheme {
	case "ws":
		fallthrough
	case "wss":
		conn = &mqtt.WebsocketConn{Host: broker}
	case "tcp":
		conn = &mqtt.TCPConn{Host: u.Host}
	default:
		log.Fatal("Invalid scheme name")
	}
	var opts []mqtt.ClientOption
	opts = append(opts, mqtt.WithCleanStart(true))
	opts = append(opts, mqtt.WithKeepAlive(uint16(5)))
	opts = append(opts, mqtt.WithClientID("client-test"))

	client := mqtt.NewClient(conn, opts...)
	connack, err := client.Connect(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Broker returned CONNACK - %s\n", connack)

	log.Println("Start consumer")
	for i := 1; i < 10; i++ {
		csg.Add(1)
		// execute on
		go consumer(client, fmt.Sprintf("topic-%d", i), i, qosLevel)
	}

	csg.Wait()
	//all consumer are started
	log.Println("Start producer")
	for i := 1; i < 10; i++ {
		psg.Add(1)
		// execute on
		go producer(client, fmt.Sprintf("topic-%d", i), i, qosLevel)
	}
	//wait untile producer has terminate the work
	psg.Wait()
	client.Disconnect(context.Background(), mqtt.DisconnectReasonCodeNormal, nil)
}
