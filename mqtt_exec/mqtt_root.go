package mqtt_exec

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/srishina/mqtt.go"
	bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var csg sync.WaitGroup
var csgEnd sync.WaitGroup

type Payload struct {
	StartTS int64            `bson:"startTS"`
	Buffer  primitive.Binary `bson:"buffer"`
}

func consumer(c mqtt.Client, topic string, counter int, qosLevel int64, samplePacketNumber uint) {
	var payload Payload
	var latency int64 = 0
	var sampleCounter uint = 0
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
			break
		}
		if bytes.Equal([]byte("END-TEST"), p.Payload) {
			break
		}
		sampleCounter++
		bson.Unmarshal(p.Payload, &payload)
		latency := latency + (time.Now().UnixMilli() - payload.StartTS)
		if sampleCounter >= samplePacketNumber {
			sampleCounter = 0
			latency = 0
			fmt.Printf("CONSUMER %d recvd(%dmsec)\n", counter, latency/int64(samplePacketNumber))
		}

	}
	log.Printf("Consumer-%d Exit\n", counter)
	csgEnd.Done()
}

func producer(c mqtt.Client, topic string, counter int, qosLevel int64, iteration int) {
	for i := 0; i < iteration; i++ {
		payload := Payload{
			StartTS: time.Now().UnixMilli(),
			Buffer: primitive.Binary{
				Subtype: 0,
				Data:    []byte("END-TEST"),
			},
		}
		b, err := bson.Marshal(payload)
		if err != nil {
			log.Fatal(err)
		}
		err = c.Publish(context.Background(), topic, byte(qosLevel), false, b, nil)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := c.Publish(context.Background(), topic, byte(qosLevel), false, []byte("END-TEST"), nil)
	if err != nil {
		log.Fatal(err)
	}
}

// TestConfig ...
type TestConfig struct {
	Broker               string
	Topic                string
	Qos                  string
	InstanceNumber       int
	IterationForInstance int
	SamplePacketNumber   int
}

// ExecuteTest ...
func ExecuteTest(config *TestConfig) {
	u, err := url.Parse(config.Broker)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	qosLevel, err := strconv.ParseInt(config.Qos, 10, 64)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	var conn mqtt.Connection
	switch u.Scheme {
	case "ws":
		fallthrough
	case "wss":
		conn = &mqtt.WebsocketConn{Host: config.Broker}
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
	for i := 1; i == config.InstanceNumber; i++ {
		csg.Add(1)
		csgEnd.Add(1)
		// execute on
		go consumer(client, fmt.Sprintf("topic-%d", i), i, qosLevel, uint(config.SamplePacketNumber))
	}

	csg.Wait()
	//all consumer are started
	log.Println("Start producer")
	for i := 1; i == config.InstanceNumber; i++ {
		// execute on
		go producer(client, fmt.Sprintf("topic-%d", i), i, qosLevel, config.IterationForInstance)
	}

	csgEnd.Wait()
	for i := 1; i == config.InstanceNumber; i++ {
		withTimeOut, cancelFn := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		defer cancelFn()
		_, err = client.Unsubscribe(withTimeOut, []string{fmt.Sprintf("topic-%d", i)}, nil)
		if err != nil {
			log.Println("UNSUBSCRIBE returned error ", err)
		}
	}

	//wait untile producer has terminate the work
	client.Disconnect(context.Background(), mqtt.DisconnectReasonCodeNormal, nil)

}
