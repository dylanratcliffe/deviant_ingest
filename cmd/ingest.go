package cmd

import (
	"fmt"
	"strings"
	"time"

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
		viper.SetDefault("dgraph.connectTimeout", "5s")

		dgHost := viper.GetString("dgraph.host")
		dgTimeout := viper.GetString("dgraph.connectTimeout")
		dgPort := viper.GetInt("dgraph.port")

		t, _ := time.ParseDuration(dgTimeout)

		// Make the dgraph connection
		dg, err := ingest.NewDGraphClient(dgHost, dgPort, t)

		viper.SetDefault("ingest.batchSize", 50)
		viper.SetDefault("ingest.maxWait", "10s")

		maxWait, _ := time.ParseDuration(viper.GetString("ingest.maxWait"))

		ir := ingest.Ingestor{
			BatchSize: viper.GetInt("ingest.batchSize"),
			MaxWait:   maxWait,
			Dgraph:    dg,
		}

		if err != nil {
			log.Fatal(err)
		}

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

		// We dont' want to do anything with the debugging info so discard it
		dc := make(chan ingest.UpsertResult)

		go func() {
			for range dc {
				// Do nothing with the results
			}
		}()

		sub, err := nc.QueueSubscribe(subject, queueName, ir.AsyncHandle)
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
