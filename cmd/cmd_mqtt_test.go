package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var mqttTestCMD = &cobra.Command{
	Use:   "mqtt",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:
Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting mqtt pub/sub test")
	},
}

// Initialize the flag
func init() {
	rootCmd.AddCommand(mqttTestCMD)
	// mqttTestCMD.Flags().IntP("instances", "n", 1, "The number of producer-consumer instance to use")
	// mqttTestCMD.Flags().IntP("iteration", "i", 10000, "Is the number of message to send")
	// mqttTestCMD.Flags().IntP("sample-count", "s", 10000, "The number of packed after wich latency is sampled")

}
