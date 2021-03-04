package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dylanratcliffe/redacted_dgraph/ingest"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Settings for the ingestion process
type Settings struct {
	NATS   NATSSettings
	Dgraph DgraphSettings
	Ingest IngestSettings
}

// NATSSettings Settings for the NATS connection
type NATSSettings struct {
	ConnectionRetries int // Times to retry connection, 0 == infinite
	ConnectionTimeout time.Duration
	URLs              []string
}

// DgraphSettings Settings for connecting to dgraph
type DgraphSettings struct {
	Host           string
	Port           int
	ConnectTimeout time.Duration
}

// IngestSettings Settings for actually ingesting
type IngestSettings struct {
	MaxWait   time.Duration
	BatchSize int
}

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Starts Ingesting items into DGraph",
	Long: `
TODO`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		var nc *nats.Conn
		var dg *dgo.Dgraph

		settings := Settings{
			NATS: NATSSettings{
				ConnectionRetries: viper.GetInt("nats.retries"),
				ConnectionTimeout: viper.GetDuration("nats.timeout"),
				URLs:              viper.GetStringSlice("nats.urls"),
			},
			Dgraph: DgraphSettings{
				Host:           viper.GetString("dgraph.host"),
				Port:           viper.GetInt("dgraph.port"),
				ConnectTimeout: viper.GetDuration("dgraph.connectTimeout"),
			},
			Ingest: IngestSettings{
				MaxWait:   viper.GetDuration("ingest.maxWait"),
				BatchSize: viper.GetInt("ingest.batchSize"),
			},
		}

		err = errors.New("")

		// Loop forever until it's connected as is the way for containers
		for err != nil {
			nc, dg, err = ConnectAll(settings)

			if err != nil {
				log.Error(err)
				time.Sleep(2 * time.Second)
			}
		}

		ir := ingest.Ingestor{
			BatchSize: settings.Ingest.BatchSize,
			MaxWait:   settings.Ingest.MaxWait,
			Dgraph:    dg,
		}

		// Subscribe
		queueName := strings.Join([]string{
			"ingest",
			settings.Dgraph.Host,
			fmt.Sprint(settings.Dgraph.Port),
		},
			".",
		)
		subject := "items.>"

		log.WithFields(log.Fields{
			"subject":   subject,
			"queueName": queueName,
		}).Info("Subscribing to item queue")

		// We don't want to do anything with the debugging info so discard it
		dc := make(chan ingest.UpsertResult)

		go func() {
			for range dc {
				// Do nothing with the results
			}
		}()

		// Start the batch processor
		batchContext, batchCancel := context.WithCancel(context.Background())
		defer batchCancel()
		go ir.ProcessBatches(batchContext)

		sub, err := nc.QueueSubscribe(subject, queueName, ir.AsyncHandle)
		defer sub.Drain()

		if err != nil {
			log.Error(err)
		}

		select {}
	},
}

// ConnectAll Connects to all the things
func ConnectAll(s Settings) (*nats.Conn, *dgo.Dgraph, error) {
	// Ensure that a NATS url was passed
	if len(s.NATS.URLs) == 0 {
		panic("No nats.urls found, this is a required setting")
	}

	var nc *nats.Conn
	var dg *dgo.Dgraph
	var err error

	hostname, _ := os.Hostname()

	urls := strings.Join(s.NATS.URLs, ",")

	// Make the NATS connection
	nc, err = nats.Connect(
		urls,                                   // The servers to connect to
		nats.Name(hostname),                    // The connection name
		nats.Timeout(s.NATS.ConnectionTimeout), // Connection timeout (per server)
	)

	if err != nil {
		return nil, nil, fmt.Errorf("Error connecting to NATS with settings: %+v. Error: %v", s.NATS, err)
	}

	// Make the dgraph connection
	dg, err = ingest.NewDGraphClient(
		s.Dgraph.Host,
		s.Dgraph.Port,
		s.Dgraph.ConnectTimeout,
	)

	if err != nil {
		return nil, nil, fmt.Errorf("Error connecting to Dgraph with settings: %+v. Error: %v", s.Dgraph, err)
	}

	log.Info("Setting up initial schemas")

	// Ensure that the schema exists
	err = ingest.SetupSchemas(dg)

	if err != nil {
		return nil, nil, err
	}

	return nc, dg, nil
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
