package cmd

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/bisegni/mqtt-test/mqtt_exec"
	"github.com/bisegni/mqtt-test/mqtt_exec_rmq"
	"github.com/spf13/cobra"
)

var mqttTest = &cobra.Command{
	Use:   "mqtt",
	Short: "Execute pu/sub test using mqtt",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 3 {
			fmt.Println("The number of argument must be three broker, topic, qos")
		}

		is, _ := cmd.Flags().GetInt("instances")
		ifi, _ := cmd.Flags().GetInt("iteration")
		spn, _ := cmd.Flags().GetInt("sample-count")
		var config = &mqtt_exec.TestConfig{
			Broker:               args[0],
			Topic:                args[1],
			Qos:                  args[2],
			InstanceNumber:       is,
			IterationForInstance: ifi,
			SamplePacketNumber:   spn,
		}
		mqtt_exec.ExecuteTest(config)
	},
}

var mqttTest2 = &cobra.Command{
	Use:   "rmq",
	Short: "Execute pu/sub test using mqtt",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 4 {
			fmt.Println("The number of argument must be four broker, topic, qos, maxPayloadSize")
		}
		qosLevel, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		maxPayloadSize, err := strconv.ParseInt(args[3], 10, 64)
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
			MaxPayloasSize:       maxPayloadSize,
		}
		mqtt_exec_rmq.ExecuteTest(config)
	},
}

// Initialize the flag
func init() {
	rootCmd.AddCommand(mqttTest)
	mqttTest.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	mqttTest.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	mqttTest.Flags().IntP("sample-count", "s", 1000, "The number of packed after wich latency is sampled")

	rootCmd.AddCommand(mqttTest2)
	mqttTest2.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	mqttTest2.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	mqttTest2.Flags().IntP("sample-count", "s", 1000, "The number of packed after wich latency is sampled")
}
