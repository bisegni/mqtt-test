package cmd

import (
	"fmt"

	"github.com/bisegni/mqtt-test/mqtt_exec"
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

// Initialize the flag
func init() {
	rootCmd.AddCommand(mqttTest)
	mqttTest.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	mqttTest.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	mqttTest.Flags().IntP("sample-count", "s", 1000, "The number of packed after wich latency is sampled")
}
