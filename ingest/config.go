package ingest

import (
	"fmt"
	"os"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/viper"
)

// InitConfigFunc Returns a function that calls InitConfig(cfgFile)
func InitConfigFunc(cfgFile string) func() {
	return func() {
		InitConfig(cfgFile)
	}
}

// InitConfig reads in config file and ENV variables if set and initialises all
// other config
func InitConfig(cfgFile string) {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.AddConfigPath(".")       // optionally look for config in the working directory
		viper.SetConfigName("deviant") // name of config file (without extension)
		viper.SetConfigType("yaml")    // REQUIRED if the config file does not have the extension in the name
	}

	replacer := strings.NewReplacer(".", "_")

	viper.SetEnvPrefix("deviant")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	// Load defaults
	SetConfigDefaults()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debugf("Using config file: %v", viper.ConfigFileUsed())
	}
}

// SetConfigDefaults Registers default values for config with Viper. This should
// always be called at some point before trying to do anything with this library
func SetConfigDefaults() {
	viper.SetDefault("dgraph.host", "localhost")
	viper.SetDefault("dgraph.port", 9080)
	viper.SetDefault("dgraph.connectTimeout", "5s")
	viper.SetDefault("dgraph.upsertTimeout", "20s")
	viper.SetDefault("dgraph.upsertRetries", 3)

	viper.SetDefault("nats.retries", 5)
	viper.SetDefault("nats.timeout", "10s")
	viper.SetDefault("nats.urls", []string{"localhost"})

	viper.SetDefault("ingest.batchSize", 250)
	viper.SetDefault("ingest.maxWait", "2s")
}
