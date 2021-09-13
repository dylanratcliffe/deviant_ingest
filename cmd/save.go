package cmd

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/dylanratcliffe/deviant_ingest/ingest"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// saveCmd represents the save command
var saveCmd = &cobra.Command{
	Use:   "save",
	Short: "Saves items to disk",
	Long:  `Saves items from a NATS network to disk for usae later`,
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to the NATS infrastructure
		urls := viper.GetStringSlice("nats.urls")

		// Ensure that a NATS url was passed
		if len(urls) == 0 {
			panic("No nats.urls found, this is a required setting")
		}

		retries := viper.GetInt("nats.retries")
		timeout := viper.GetDuration("nats.timeout")

		// Make the NATS connection
		nc := ingest.NewNATSConnection(
			urls,
			retries,
			5,
			timeout,
		)

		sigs := make(chan os.Signal, 1)
		done := make(chan bool, 1)

		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			sig := <-sigs
			fmt.Println("Cancelled, writing to disk")
			fmt.Println(sig)
			done <- true
		}()

		subject := cmd.Flag("subject").Value.String()

		sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
			data := msg.Data
			filename := fmt.Sprintf("%v/%x.pb", cmd.Flag("out").Value, sha1.Sum(data))
			fmt.Sprintln(filename)
			if err := ioutil.WriteFile(filename, data, 0644); err != nil {
				fmt.Sprintln(err)
			}
		})

		if err != nil {
			fmt.Println(err)
		}

		<-done
		sub.Drain()
	},
}

func init() {
	rootCmd.AddCommand(saveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	saveCmd.PersistentFlags().String("subject", "items.>", "The NATS subject to listen on")
	saveCmd.PersistentFlags().String("out", "items", "Name of the directory to save files to")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// saveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
