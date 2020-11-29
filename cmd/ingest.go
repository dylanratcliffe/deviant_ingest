package cmd

import (
	"fmt"
	"strings"

	"github.com/dylanratcliffe/redacted_dgraph/ingest"
	log "github.com/sirupsen/logrus"
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
		var err error

		// Connect to the NATS infrastructure
		viper.SetDefault("nats.retries", 5)
		viper.SetDefault("nats.timeout", 10)
		viper.SetDefault("nats.urls", []string{"localhost"})

		urls := viper.GetStringSlice("nats.urls")

		// Ensure that a NATS url was passed
		if len(urls) == 0 {
			panic("No nats.urls found, this is a required setting")
		}

		retries := viper.GetInt("nats.retries")
		timeout := viper.GetInt("nats.timeout")

		// Make the NATS connection
		nc := ingest.NewNATSConnection(
			urls,
			retries,
			5,
			timeout,
		)

		viper.SetDefault("dgraph.host", "localhost")
		viper.SetDefault("dgraph.port", 9080)
		dgHost := viper.GetString("dgraph.host")
		dgPort := viper.GetInt("dgraph.port")

		// Make the dgraph connection
		dg := ingest.NewDGraphClient(dgHost, dgPort)

		log.Info("Setting up initial schemas")

		// Ensure that the schema exists
		err = ingest.SetupSchemas(dg)

		if err != nil {
			log.WithFields(log.Fields{
				"schemas": ingest.Schema,
				"error":   err,
			}).Fatal("Failed to set up initial schemas")
		}

		// Subscribe
		queueName := strings.Join([]string{
			"ingest",
			dgHost,
			fmt.Sprint(dgPort),
		},
			".",
		)
		subject := "items.>"

		log.WithFields(log.Fields{
			"subject":   subject,
			"queueName": queueName,
		}).Info("Subscribing to item queue")

		sub, err := nc.QueueSubscribe(subject, queueName, ingest.NewUpsertHandler(dg))
		defer sub.Drain()

		if err != nil {
			log.Error(err)
		}

		select {}
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
