package ingest

import (
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// DGraph Stores details about the DGraph connection
type DGraph struct {
	Conn *dgo.Dgraph
}

// InsertAllHandler Nats item handler that inserts items into the database
func (d *DGraph) InsertAllHandler(msg *nats.Msg) {

}

func newDGraphClient(hostname string, port int) *dgo.Dgraph {
	address := fmt.Sprintf("%v:%v", hostname, port)

	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	d, err := grpc.Dial(address, dialOpts...)

	if err != nil {
		log.Fatal(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

// ConnectToNats connects to a given NATS URL, it also support retries. Servers should be supplied as a slice of URLs e.g.
//
// engine.ConnectToNats([]string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223"},	5, 5)
//
func newNATSConnection(urls []string, retries int, timeout int) *nats.Conn {
	var tries int
	var servers string
	var hostname string
	var timeoutDuration time.Duration

	// Set default values
	if retries == 0 {
		retries = 10
	}

	if timeout == 0 {
		timeoutDuration = 10 * time.Second
	} else {
		timeoutDuration = time.Duration(timeout) * time.Second

	}

	// Get the hostname to use as the connection name
	hostname, _ = os.Hostname()

	servers = strings.Join(urls, ",")

	// Loop until we have a connection
	for tries <= retries {
		log.WithFields(log.Fields{
			"servers": servers,
		}).Info("Connecting to NATS")

		// TODO: Make these options more configurable
		// https://docs.nats.io/developing-with-nats/connecting/pingpong
		nc, err := nats.Connect(
			servers,                       // The servers to connect to
			nats.Name(hostname),           // The connection name
			nats.Timeout(timeoutDuration), // Connection timeout (per server)
		)

		if err == nil {
			return nc
		}

		// Increment tries
		tries++

		log.WithFields(log.Fields{
			"servers": servers,
			"err":     err,
		}).Info("Connection failed")
	}

	panic("Could not connect to NATS, giving up")
}
