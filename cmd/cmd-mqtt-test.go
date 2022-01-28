package cmd

import (
	"log"
	"os"

	"github.com/bisegni/mqtt-test/mqtt_test"
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
		mqtt_test.ExecuteTest(args[0], args[1], args[2])
	},
}

// Initialize the flag
func init() {
	rootCmd.AddCommand(mqttTestCMD)
}
