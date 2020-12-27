package ingest

import "github.com/spf13/viper"

// SetConfigDefaults Registers default values for config with Viper. This should
// always be called at some point before trying to do anything with this library
func SetConfigDefaults() {
	viper.SetDefault("dgraph.host", "localhost")
	viper.SetDefault("dgraph.port", 9080)
	viper.SetDefault("dgraph.connectTimeout", "5s")
	viper.SetDefault("dgraph.upsertTimeout", "20s")
	viper.SetDefault("dgraph.upsertRetries", 3)

	viper.SetDefault("nats.retries", 5)
	viper.SetDefault("nats.timeout", 10)
	viper.SetDefault("nats.urls", []string{"localhost"})

	viper.SetDefault("ingest.batchSize", 250)
	viper.SetDefault("ingest.maxWait", "10s")
}
