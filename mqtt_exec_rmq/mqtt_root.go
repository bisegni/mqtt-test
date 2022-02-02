package mqtt_exec_rmq

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"image/color"
	"log"
	"math"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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

func consumerClose(channelClose stream.ChannelClose) {
	event := <-channelClose
	fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}
func consumer(conn *stream.Environment, topic string, counter int, config *TestConfig) {
	var payload Payload
	var latency int64 = 0
	var lastCounter int64 = 0
	var lostPackage int64 = 0
	var globalCounter int64 = 0
	var payloadPacketReceived int64 = 0
	var payloadSizeReceived int64 = 0
	var payloadSizeMean float64 = 0
	var payloadSizeMeanSample int64 = 0
	var payloadLastTS int64 = time.Now().UnixMilli()
	var statistic Statistic
	var producerResp *stream.Producer
	var consumer *stream.Consumer

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		if bytes.Equal([]byte("END-TEST"), message.GetData()) {
			//stop work
			consumer.Close()
			csgEnd.Done()
			return
		} else if bytes.Equal([]byte("END-ITERATION"), message.GetData()) {
			statistic.ConsumerThroughputMean = float64(payloadSizeMean) / float64(payloadSizeMeanSample)
			if math.IsNaN(statistic.ConsumerThroughputMean) {
				statistic.ConsumerThroughputMean = 0
			}
			statistic.ConsumerLatency = float64(latency) / float64(globalCounter)
			statistic.ConsumerLostPacket = lostPackage
			statistic.ConsumerReceivedPacket = globalCounter
			statSer, _ := json.Marshal(statistic)

			message := amqp.NewMessage([]byte(statSer))
			err := producerResp.Send(message)
			failOnError(err, "Failed to open a channel")
			// go to next test
			latency = 0
			globalCounter = 0
			lastCounter = 0
			lostPackage = 0
			return
		}
		globalCounter++
		bson.Unmarshal(message.GetData(), &payload)

		if (lastCounter + 1) != payload.Counter {
			lostPackage++
		}
		sampleTime := time.Now().UnixMilli()
		lastCounter = payload.Counter
		latency = latency + (time.Now().UnixMilli() - payload.StartTS)
		// throughput compute
		payloadPacketReceived++
		payloadSizeReceived = payloadSizeReceived + int64(len(message.GetData()))
		// throughput
		if sampleTime-payloadLastTS >= 1000 {
			// sample throughput
			payloadSizeMeanSample++
			payloadSizeMean = payloadSizeMean + (float64(payloadSizeReceived) / float64(payloadPacketReceived))
			payloadSizeReceived = 0
			payloadPacketReceived = 0
			payloadLastTS = sampleTime
		}
	}

	err := conn.DeclareStream(
		topic,
		stream.NewStreamOptions().
			SetMaxAge(time.Duration(time.Duration(1).Milliseconds())).
			SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)),
	)
	failOnError(err, "Failed to open a channel")
	err = conn.DeclareStream(
		topic+"-resp",
		stream.NewStreamOptions().
			SetMaxAge(time.Duration(time.Duration(1).Milliseconds())).
			SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)),
	)
	failOnError(err, "Failed to open a channel")
	consumer, err = conn.NewConsumer(
		topic,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName(topic+"-consumer").
			SetAutoCommit(stream.NewAutoCommitStrategy().
				SetCountBeforeStorage(1). // each 50 messages stores the index
				SetFlushInterval(1*time.Second)).
			SetOffset(stream.OffsetSpecification{}.Timestamp(time.Now().UnixMilli())).
			SetCRCCheck(false))
	failOnError(err, "Failed to open a channel")
	channelClose := consumer.NotifyClose()
	defer consumerClose(channelClose)

	producerResp, err = conn.NewProducer(
		topic+"-resp",
		&stream.ProducerOptions{
			Name:                 topic + "-producer",
			QueueSize:            100,
			BatchSize:            50,
			BatchPublishingDelay: 50,
			SubEntrySize:         1,
		},
	)
	failOnError(err, "Failed to open a channel")
	csg.Done()
}

func getPayload(size int64) []byte {
	token := make([]byte, size)
	rand.Read(token)
	return token
}

func producer(conn *stream.Environment, topic string, counter int, payloadByteSize int64, config *TestConfig) {
	producer, err := conn.NewProducer(
		topic,
		stream.NewProducerOptions().
			SetSubEntrySize(500).
			SetCompression(stream.Compression{}.None()),
	)
	failOnError(err, "Failed to open a channel")
	defer producer.Close()

	var latency int64 = 0
	var sampleCounter uint = 0
	var globalCounter int64 = 0
	var payloadPacketSent int64 = 0
	var payloadSizeSent int64 = 0
	var payloadSizeMean float64 = 0
	var payloadSizeMeanSample int64 = 0
	var payloadLastTS int64 = time.Now().UnixMilli()
	var statistic Statistic

	for i := 0; i < config.IterationForInstance; i++ {
		startTS := time.Now().UnixMilli()
		globalCounter++
		payload := Payload{
			StartTS: startTS,
			Counter: globalCounter,
			Buffer: primitive.Binary{
				Subtype: 0,
				Data:    getPayload(payloadByteSize),
			},
		}
		b, err := bson.Marshal(payload)
		if err != nil {
			log.Fatal(err)
		}
		message := amqp.NewMessage([]byte(b))
		err = producer.Send(message)
		failOnError(err, "Failed to publish a message")

		sampleTime := time.Now().UnixMilli()
		// calculate latency
		sampleCounter++
		latency = latency + (sampleTime - startTS)

		payloadPacketSent++
		payloadSizeSent = payloadSizeSent + int64(len(b))
		// throughput
		if sampleTime-payloadLastTS >= 1000 {
			// sample throughput
			payloadSizeMeanSample++
			payloadSizeMean = payloadSizeMean + (float64(payloadSizeSent) / float64(payloadPacketSent))
			payloadSizeSent = 0
			payloadPacketSent = 0
			payloadLastTS = sampleTime
		}
	}

	message := amqp.NewMessage([]byte("END-ITERATION"))
	err = producer.Send(message)
	failOnError(err, "Failed to declare a queue")
	var waitForAnswer sync.WaitGroup
	waitForAnswer.Add(1)
	consumer, err := conn.NewConsumer(
		topic+"-resp",
		func(consumerContext stream.ConsumerContext, message *amqp.Message) {
			_ = json.Unmarshal(message.GetData(), &statistic)
			waitForAnswer.Done()
		},
		stream.NewConsumerOptions().
			SetConsumerName(topic+"-resp-consumer").
			SetAutoCommit(nil).
			SetOffset(stream.OffsetSpecification{}.Timestamp(time.Now().UnixMilli())).
			SetCRCCheck(false))
	failOnError(err, "Failed to open a channel")
	//get response
	waitForAnswer.Wait()
	consumer.Close()
	statistic.ProducerThroughputMean = float64(payloadSizeMean) / float64(payloadSizeMeanSample)
	if math.IsNaN(statistic.ProducerThroughputMean) {
		statistic.ProducerThroughputMean = 0
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
	ProducerThroughputMean float64
	ConsumerLatency        float64
	ConsumerLostPacket     int64
	ConsumerReceivedPacket int64
	ConsumerThroughputMean float64
}

type PlotInfo struct {
	packetSize int64
	pLat       float64
	cLat       float64
}

var threadOutput []Statistic

// ExecuteTest ...
func ExecuteTest(config *TestConfig) {
	addresses := []string{
		config.Broker,
	}
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUris(addresses),
	)
	failOnError(err, "Failed to connect to RabbitMQ")

	for i := 1; i <= config.InstanceNumber; i++ {
		csg.Add(1)
		csgEnd.Add(1)
		// execute on
		go consumer(env, fmt.Sprintf("topic-%d", i), i, config)
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
			go producer(env, fmt.Sprintf("topic-%d", i), i, packetSize, config)
		}
		// waith for all producer end to sned data
		psgEnd.Wait()

		// print output statistic and plot
		var meanCLat = float64(0)
		var meanPLat = float64(0)
		for i, stat := range threadOutput {
			fmt.Printf(
				"Index: %d, plat: %f, ptot: %d, pthroughput: %s/sec, clat: %f, clos: %d, ctot: %d, cthroughput: %s/sec\n",
				i,
				stat.ProducerLatency,
				stat.ProducerSentPacket,
				ByteCountSI(int64(stat.ProducerThroughputMean)),
				stat.ConsumerLatency,
				stat.ConsumerLostPacket,
				stat.ConsumerReceivedPacket,
				ByteCountSI(int64(stat.ConsumerThroughputMean)),
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

	for i := 1; i <= config.InstanceNumber; i++ {
		//signal the end of work to the consumer
		for i := 1; i <= config.InstanceNumber; i++ {
			producerEndMessage, err := env.NewProducer(
				fmt.Sprintf("topic-%d", i),
				&stream.ProducerOptions{
					Name:                 fmt.Sprintf("topic-%d", i) + "-producer-end-emssage",
					QueueSize:            10,
					BatchSize:            10,
					BatchPublishingDelay: 0,
				},
			)
			failOnError(err, "Failed to open a channel")
			err = producerEndMessage.Send(amqp.NewMessage([]byte("END-TEST")))

			failOnError(err, "Failed to publish a message")
		}
	}
	csgEnd.Wait()
	env.Close()
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
