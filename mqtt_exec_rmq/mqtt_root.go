package mqtt_exec_rmq

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"image/color"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
	bson "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
)

var csg sync.WaitGroup
var csgEnd sync.WaitGroup
var psgEnd sync.WaitGroup

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
	var statistic Statistic

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
	for m := range msgs {
		if bytes.Equal([]byte("END-TEST"), m.Body) {
			//stop work
			break
		}
		if bytes.Equal([]byte("END-ITERATION"), m.Body) {

			statistic.ConsumerLatency = float64(latency) / float64(globalCounter)
			statistic.ConsumerLostPacket = lostPackage
			statistic.ConsumerReceivedPacket = globalCounter
			statSer, _ := json.Marshal(statistic)

			err = ch.Publish(
				"",        // exchange
				m.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: m.CorrelationId,
					Body:          statSer,
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
	csgEnd.Done()
}

func getPayload(size int64) []byte {
	token := make([]byte, size)
	rand.Read(token)
	return token
}

func producer(conn *amqp.Connection, topic string, counter int, payloadByteSize int64, config *TestConfig) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	var latency int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0
	var currentByteSize int64 = 1
	var statistic Statistic
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
			_ = json.Unmarshal(d.Body, &statistic)
			break
		}
	}
	statistic.ProducerLatency = float64(latency) / float64(globalCounter)
	statistic.ProducerSentPacket = globalCounter
	threadOutput[counter-1] = statistic
	psgEnd.Done()
}

// TestConfig ...
type TestConfig struct {
	Broker               string
	Topic                string
	Qos                  int
	InstanceNumber       int
	IterationForInstance int
	SamplePacketNumber   int
	RaisedTo             int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type Statistic struct {
	ProducerLatency        float64
	ProducerSentPacket     int64
	ConsumerLatency        float64
	ConsumerLostPacket     int64
	ConsumerReceivedPacket int64
}

type PlotInfo struct {
	packetSize int64
	pLat       float64
	cLat       float64
}

var threadOutput []Statistic

// ExecuteTest ...
func ExecuteTest(config *TestConfig) {
	conn, err := amqp.Dial(config.Broker)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	for i := 1; i <= config.InstanceNumber; i++ {
		csg.Add(1)
		csgEnd.Add(1)
		// execute on
		go consumer(conn, fmt.Sprintf("topic-%d", i), i, config)
	}
	csg.Wait()

	//create output array for consumer
	threadOutput = make([]Statistic, config.InstanceNumber)
	var plotInstances []PlotInfo
	run := 0

	fmt.Printf("Start test up to %s\n", ByteCountSI(int64(math.Pow(2, float64(config.RaisedTo)))))

	for packetSize := int64(1); packetSize <= int64(math.Pow(2, float64(config.RaisedTo))); packetSize = packetSize << 1 {
		fmt.Printf("------------ packet size: %s --------------\n", ByteCountSI(packetSize))
		// star producer
		for i := 1; i <= config.InstanceNumber; i++ {
			psgEnd.Add(1)
			// execute on
			go producer(conn, fmt.Sprintf("topic-%d", i), i, packetSize, config)
		}
		// waith for all producer end to sned data
		psgEnd.Wait()

		// print output statistic and plot
		var meanCLat = float64(0)
		var meanPLat = float64(0)
		for i, stat := range threadOutput {
			fmt.Printf(
				"Index: %d plat: %f ptot: %d clat: %f, clos: %d, ctot: %d\n",
				i,
				stat.ProducerLatency,
				stat.ProducerSentPacket,
				stat.ConsumerLatency,
				stat.ConsumerLostPacket,
				stat.ConsumerReceivedPacket,
			)
			meanPLat = meanPLat + stat.ProducerLatency
			meanCLat = meanCLat + stat.ConsumerLatency
		}

		plotInstances = append(plotInstances, PlotInfo{
			cLat:       meanCLat / float64(len(threadOutput)),
			pLat:       meanPLat / float64(len(threadOutput)),
			packetSize: packetSize,
		})
		run++
	}

	cmdChannel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	for i := 1; i <= config.InstanceNumber; i++ {
		//signal the end of work to the consumer
		for i := 1; i <= config.InstanceNumber; i++ {
			err = cmdChannel.Publish(
				"input-gateway",            // exchange
				fmt.Sprintf("topic-%d", i), // routing key
				false,                      // mandatory
				false,                      // immediate
				amqp.Publishing{
					ContentType: "bson",
					Body:        []byte("END-TEST"),
				})
			failOnError(err, "Failed to publish a message")
		}
	}
	csgEnd.Wait()
	conn.Close()
	fmt.Println("Generating plot")
	plotStatistic(plotInstances)
}

func plotStatistic(plotInfo []PlotInfo) {
	p := plot.New()

	p.Title.Text = "Latency Plot"
	p.X.Label.Text = "Packet Size(KB)"
	p.Y.Label.Text = "Latency"

	producerLatencyData := make(plotter.XYs, len(plotInfo))
	consumerLatencyData := make(plotter.XYs, len(plotInfo))

	for i := 0; i < len(plotInfo); i++ {
		producerLatencyData[i].X = float64(plotInfo[i].packetSize) / 1024
		consumerLatencyData[i].X = float64(plotInfo[i].packetSize) / 1024

		producerLatencyData[i].Y = plotInfo[i].pLat
		consumerLatencyData[i].Y = plotInfo[i].cLat
	}

	p.Add(plotter.NewGrid())
	l1, err := plotter.NewLine(producerLatencyData)
	failOnError(err, "Failed to create line one plotter")
	l1.LineStyle.Color = color.RGBA{G: 255, A: 255}
	l2, err := plotter.NewLine(consumerLatencyData)
	failOnError(err, "Failed to create line two plotter")
	p.Add(l1, l2)
	l2.LineStyle.Color = color.RGBA{R: 255, A: 255}
	p.Legend.Add("Producer", l1)
	p.Legend.Add("Consumer", l2)
	if err := p.Save(1024, 1024, "plot.png"); err != nil {
		panic(err)
	}
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
