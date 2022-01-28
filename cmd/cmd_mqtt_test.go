package cmd

import (
	"log"
	"os"

	"github.com/bisegni/mqtt-test/mqtt_exec"
	"github.com/spf13/cobra"
)

var mqttTestCMD = &cobra.Command{
	Use:   "mqtt",
	Short: "Execute mqtt test",
	Long:  `Execute test on mqtt pub/sub tech`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 3 {
			log.Fatal("mqtt comamnd need broker topic and qosLevel paramter")
			os.Exit(1)
		}

		in, _ := cmd.Flags().GetInt("instances")
		iter, _ := cmd.Flags().GetInt("iteration")
		sc, _ := cmd.Flags().GetInt("sample-count")
		var config = &mqtt_exec.TestConfig{
			Broker:               args[0],
			Topic:                args[1],
			Qos:                  args[2],
			InstanceNumber:       in,
			IterationForInstance: iter,
			SamplePacketNumber:   sc,
		}
		mqtt_exec.ExecuteTest(config)
	},
}

// Initialize the flag
func init() {
	mqttTestCMD.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	mqttTestCMD.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	mqttTestCMD.Flags().IntP("sample-count", "s", 10000, "The number of packed after wich latency is sampled")
	rootCmd.AddCommand(mqttTestCMD)
}
