package cmd

import (
	"fmt"
	"github.com/dylanratcliffe/redacted_dgraph/ingest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Starts Ingesting items into DGraph",
	Long: `
TODO`,
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to the NATS infrastructure
		viper.SetDefault("nats.retries", 5)
		viper.SetDefault("nats.timeout", 10)

		urls := viper.GetStringSlice("nats.urls")

		// Ensure that a NATS url was passed
		if len(urls) == 0 {
			panic("No nats.urls found, this is a required setting")
		}

		retries := viper.GetInt("nats.retries")
		timeout := viper.GetInt("nats.timeout")

		// Make the NATS connection
		c := ingest.NewNATSConnection(
			urls,
			retries,
			5,
			timeout,
		)

		// Subscribe
		fmt.Println(c)
	},
}

func init() {
	rootCmd.AddCommand(ingestCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// ingestCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// ingestCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
