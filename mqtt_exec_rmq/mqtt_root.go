package mqtt_exec_rmq

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
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

func consumer(conn *amqp.Connection, topic string, counter int, config *TestConfig) {
	var payload Payload
	var latency int64 = 0
	var lastCounter int64 = 0
	var lostPackage int64 = 0
	var globalCounter int64 = 0
	ch, err := conn.Channel()
	ch.Qos(1, 0, false)
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
			//stop work
			break
		}
		if bytes.Equal([]byte("END-ITERATION"), m.Body) {
			latMean := float64(latency) / float64(globalCounter)
			resp := fmt.Sprintf("CONSUMER %d latency(%f mSec) lost package(%d) totpkg(%d)\n", counter, latMean, lostPackage, globalCounter)
			err = ch.Publish(
				"",        // exchange
				m.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: m.CorrelationId,
					Body:          []byte(resp),
				})
			failOnError(err, "Failed to publish a message")
			// go to next test
			latency = 0
			globalCounter = 0
			lastCounter = 0
			lostPackage = 0
			continue
		}
		globalCounter++
		bson.Unmarshal(m.Body, &payload)

		if (lastCounter + 1) != payload.Counter {
			lostPackage++
		}
		lastCounter = payload.Counter
		latency = latency + (time.Now().UnixMilli() - payload.StartTS)
	}
	fmt.Printf("Consumer-%d Exit\n", counter)
	csgEnd.Done()
}

func getPayload(size int64) []byte {
	token := make([]byte, size)
	rand.Read(token)
	return token
}

func producer(conn *amqp.Connection, topic string, counter int, config *TestConfig) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	var latency int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0
	var currentByteSize int64 = 1
	var latmean float64 = 0
	var producerOut string
	var consumerOut string
	// failOnError(err, "Failed to declare a queue")
	fmt.Printf("Producer-%d-qos[%d]-Entered\n", counter, config.Qos)
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	resp, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to declare a queue")
	for currentByteSize < config.MaxPayloasSize {
		fmt.Printf("Use buffer size of %d\n", currentByteSize)
		for i := 0; i < config.IterationForInstance; i++ {
			startTS := time.Now().UnixMilli()
			globalCounter++
			payload := Payload{
				StartTS: startTS,
				Counter: globalCounter,
				Buffer: primitive.Binary{
					Subtype: 0,
					Data:    getPayload(currentByteSize),
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
					ContentType: "bson",
					Body:        b,
				})
			failOnError(err, "Failed to publish a message")

			// calculate latency
			sampleCounter++
			latency = latency + (time.Now().UnixMilli() - startTS)
		}
		latmean = float64(latency) / float64(globalCounter)
		latency = 0
		globalCounter = 0
		producerOut = fmt.Sprintf("PRODUCER %d latency(%f mSec) totpkg(%d)", counter, latmean, globalCounter)
		err = ch.Publish(
			"input-gateway", // exchange
			topic,           // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType:   "bson",
				CorrelationId: strconv.FormatInt(currentByteSize, 10),
				ReplyTo:       q.Name,
				Body:          []byte("END-ITERATION"),
			})
		failOnError(err, "Failed to declare a queue")
		//get response
		for d := range resp {
			if strconv.FormatInt(currentByteSize, 10) == d.CorrelationId {
				consumerOut = string(d.Body)
				break
			}
		}
		threadOutput[counter-1] = fmt.Sprintf("%s - %s", producerOut, consumerOut)
		// ncrement byte size
		currentByteSize = currentByteSize * 2
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
	Qos                  int
	InstanceNumber       int
	IterationForInstance int
	SamplePacketNumber   int
	MaxPayloasSize       int64
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var threadOutput []string

// ExecuteTest ...
func ExecuteTest(config *TestConfig) {
	conn, err := amqp.Dial(config.Broker)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	log.Println("Start consumer")
	for i := 1; i <= config.InstanceNumber; i++ {
		csg.Add(1)

		// execute on
		go consumer(conn, fmt.Sprintf("topic-%d", i), i, config)
	}
	csg.Wait()

	//create output array for consumer
	threadOutput = make([]string, config.InstanceNumber)
	//all consumer are started
	log.Println("Start producer")
	for i := 1; i <= config.InstanceNumber; i++ {
		csgEnd.Add(1)
		// execute on
		go producer(conn, fmt.Sprintf("topic-%d", i), i, config)
	}
	csgEnd.Wait()

	conn.Close()
}
