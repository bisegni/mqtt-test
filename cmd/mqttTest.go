package cmd

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/bisegni/mqtt-test/mqtt_exec_nats"
	"github.com/bisegni/mqtt-test/mqtt_exec_rmq"
	"github.com/spf13/cobra"
)

var rmqTest = &cobra.Command{
	Use:   "rmq",
	Short: "Execute pub/sub test using rabbitmq",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 4 {
			fmt.Println("The number of argument must be four broker, topic, qos, raised to(2 rt 4) for the max packet size")
		}
		qosLevel, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		raisedTo, err := strconv.Atoi(args[3])
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		is, _ := cmd.Flags().GetInt("instances")
		ifi, _ := cmd.Flags().GetInt("iteration")
		spn, _ := cmd.Flags().GetInt("sample-count")
		var config = &mqtt_exec_rmq.TestConfig{
			Broker:               args[0],
			Topic:                args[1],
			Qos:                  int(qosLevel),
			InstanceNumber:       is,
			IterationForInstance: ifi,
			SamplePacketNumber:   spn,
			RaisedTo:             raisedTo,
		}
		mqtt_exec_rmq.ExecuteTest(config)
	},
}

var natsTest = &cobra.Command{
	Use:   "nats",
	Short: "Execute pub/sub test using rabbitmq",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 4 {
			fmt.Println("The number of argument must be four broker, topic, qos, raised to(2 rt 4) for the max packet size")
		}
		qosLevel, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		raisedTo, err := strconv.Atoi(args[3])
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		is, _ := cmd.Flags().GetInt("instances")
		ifi, _ := cmd.Flags().GetInt("iteration")
		spn, _ := cmd.Flags().GetInt("sample-count")
		var config = &mqtt_exec_nats.TestConfig{
			Broker:               args[0],
			Topic:                args[1],
			Qos:                  int(qosLevel),
			InstanceNumber:       is,
			IterationForInstance: ifi,
			SamplePacketNumber:   spn,
			RaisedTo:             raisedTo,
		}
		mqtt_exec_nats.ExecuteTest(config)
	},
}

// Initialize the flag
func init() {
	rootCmd.AddCommand(rmqTest)
	rmqTest.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	rmqTest.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	rmqTest.Flags().IntP("sample-count", "s", 1000, "The number of packed after wich latency is sampled")

	rootCmd.AddCommand(natsTest)
	natsTest.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	natsTest.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	natsTest.Flags().IntP("sample-count", "s", 1000, "The number of packed after wich latency is sampled")
}
