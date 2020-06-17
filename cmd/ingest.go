package cmd

import (
	"fmt"
	"log"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// ingestCmd represents the ingest command
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Dial a gRPC connection. The address to dial to can be configured when
		// setting up the dgraph cluster.
		dialOpts := append([]grpc.DialOption{},
			grpc.WithInsecure(),
			grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
		d, err := grpc.Dial("localhost:9080", dialOpts...)

		if err != nil {
			log.Fatal(err)
		}

		client := dgo.NewDgraphClient(
			api.NewDgraphClient(d),
		)

		fmt.Println(client)

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
