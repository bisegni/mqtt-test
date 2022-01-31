package mqtt_exec_rmq

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
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

func consumer(conn *amqp.Connection, topic string, counter int, qosLevel int64, samplePacketNumber uint) {
	var payload Payload
	var latency int64 = 0
	var lastCounter int64 = 0
	var lostPackage int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0
	ch, err := conn.Channel()
	ch.Qos(10, 0, false)
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	ch.ExchangeDeclare(
		"input-gateway", // name
		"direct",        // type
		false,           // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,          // queue name
		topic,           // routing key
		"input-gateway", // exchange
		false,
		nil)
	failOnError(err, "Failed to declare a queue")

	// err = ch.QueueBind(
	// 	q.Name,          // queue name
	// 	"end-signal",    // routing key
	// 	"input-gateway", // exchange
	// 	false,
	// 	nil)
	// failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	//signal wait group
	csg.Done()
	fmt.Printf("Consumer-%d Entered\n", counter)
	for m := range msgs {
		if bytes.Equal([]byte("END-TEST"), m.Body) {
			break
		}
		globalCounter++
		sampleCounter++
		bson.Unmarshal(m.Body, &payload)

		if lastCounter >= payload.Counter {
			lostPackage++
		}

		latency := latency + (time.Now().UnixMilli() - payload.StartTS)
		if sampleCounter >= samplePacketNumber {
			latMean := latency / int64(samplePacketNumber)
			sampleCounter = 0
			latency = 0
			fmt.Printf("CONSUMER %d latency(%d mSec) lost package(%d) totpkg(%d)\n", counter, latMean, lostPackage, globalCounter)
		}
	}
	fmt.Printf("Consumer-%d Exit\n", counter)
	csgEnd.Done()
}

func producer(conn *amqp.Connection, topic string, counter int, qosLevel int64, iteration int, samplePacketNumber uint) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	var latency int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0

	// failOnError(err, "Failed to declare a queue")
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

		err = ch.Publish(
			"input-gateway", // exchange
			topic,           // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				//DeliveryMode: amqp.Persistent,
				ContentType: "bson",
				Body:        b,
			})
		failOnError(err, "Failed to publish a message")

		// calculate latency
		sampleCounter++
		latency = latency + (time.Now().UnixMilli() - startTS)
		if sampleCounter >= samplePacketNumber {
			//latMean := latency / int64(samplePacketNumber)
			sampleCounter = 0
			latency = 0
			//fmt.Printf("PRODUCER %d latency(%d mSec) totpkg(%d)\n", counter, latMean, globalCounter)
		}
	}

	err = ch.Publish(
		"input-gateway", // exchange
		topic,           // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "bson",
			Body:        []byte("END-TEST"),
		})
	failOnError(err, "Failed to publish a message")
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// ExecuteTest ...
func ExecuteTest(config *TestConfig) {
	qosLevel, err := strconv.ParseInt(config.Qos, 10, 64)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	conn, err := amqp.Dial(config.Broker)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	log.Println("Start consumer")
	for i := 1; i <= config.InstanceNumber; i++ {
		csg.Add(1)
		csgEnd.Add(1)

		// execute on
		go consumer(conn, fmt.Sprintf("topic-%d", i), i, qosLevel, uint(config.SamplePacketNumber))
	}
	csg.Wait()
	//all consumer are started
	log.Println("Start producer")
	for i := 1; i <= config.InstanceNumber; i++ {
		// execute on
		go producer(conn, fmt.Sprintf("topic-%d", i), i, qosLevel, config.IterationForInstance, uint(config.SamplePacketNumber))
	}
	//producer(conn, fmt.Sprintf("topic-%d", 1), 1, qosLevel, config.IterationForInstance, uint(config.SamplePacketNumber))
	csgEnd.Wait()

	conn.Close()
}
