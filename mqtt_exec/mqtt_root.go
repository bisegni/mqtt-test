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
	Counter int64            `bson:"counter"`
	Buffer  primitive.Binary `bson:"buffer"`
}

func consumer(c mqtt.Client, topic string, counter int, qosLevel int64, samplePacketNumber uint) {
	var payload Payload
	var latency int64 = 0
	var lastCounter int64 = 0
	var lostPackage int64 = 0
	var sampleCounter uint = 0
	recvr := mqtt.NewMessageReceiver()

	subscriptions := []*mqtt.Subscription{}
	subscriptions = append(subscriptions, &mqtt.Subscription{TopicFilter: topic, QoSLevel: byte(qosLevel)})

	_, err := c.Subscribe(context.Background(), subscriptions, nil, recvr)
	if err != nil {
		log.Fatal(err)
	}
	//signal wait group
	csg.Done()
	fmt.Printf("Consumer-%d Entered\n", counter)
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

		if lastCounter >= payload.Counter {
			lostPackage++
		}

		latency := latency + (time.Now().UnixMilli() - payload.StartTS)
		if sampleCounter >= samplePacketNumber {
			latMean := latency / int64(samplePacketNumber)
			sampleCounter = 0
			latency = 0
			fmt.Printf("CONSUMER %d latency(%d mSec) lost package(%d)\n", counter, latMean, lostPackage)
		}
	}
	fmt.Printf("Consumer-%d Exit\n", counter)
	csgEnd.Done()
}

func producer(c mqtt.Client, topic string, counter int, qosLevel int64, iteration int, samplePacketNumber uint) {
	var latency int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0
	fmt.Printf("Producer-%d-qos[%d]-Entered\n", counter, qosLevel)
	for i := 0; i < iteration; i++ {
		startTS := time.Now().UnixMilli()
		globalCounter++
		payload := Payload{
			StartTS: startTS,
			Counter: globalCounter,
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

		// calculate latency
		sampleCounter++
		latency = latency + (time.Now().UnixMilli() - startTS)
		if sampleCounter >= samplePacketNumber {
			latMean := latency / int64(samplePacketNumber)
			sampleCounter = 0
			latency = 0
			fmt.Printf("PRODUCER %d recvd(%d mSec)\n", counter, latMean)
		}
	}
	err := c.Publish(context.Background(), topic, byte(qosLevel), false, []byte("END-TEST"), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Producer-%d Exit\n", counter)
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
	for i := 1; i <= config.InstanceNumber; i++ {
		csg.Add(1)
		csgEnd.Add(1)
		// execute on
		go consumer(client, fmt.Sprintf("topic-%d", 1), i, qosLevel, uint(config.SamplePacketNumber))
	}

	csg.Wait()
	//all consumer are started
	log.Println("Start producer")
	for i := 1; i <= config.InstanceNumber; i++ {
		// execute on
		go producer(client, fmt.Sprintf("topic-%d", i), i, qosLevel, config.IterationForInstance, uint(config.SamplePacketNumber))
	}

	csgEnd.Wait()
	for i := 1; i <= config.InstanceNumber; i++ {
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
