package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/thathurleyguy/mongo_bench/bencher"
)

var (
	config bencher.Config

	rootCmd = &cobra.Command{
		Use:   "mongo_bench",
		Short: "Simple tool to simulate load on a mongodb cluster",
		Long:  "Simple tool to simulate load on a mongodb cluster",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			bencher := bencher.NewBencher(ctx, &config)
			if *config.MetadataURI == "" {
				config.MetadataURI = config.PrimaryURI
			}

			bencher.Start()
		},
	}
)

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	config = bencher.Config{}
	config.NumInsertWorkers = rootCmd.Flags().Int("insert-workers", 1, "Number of insert worker goroutines to run")
	config.NumIDReadWorkers = rootCmd.Flags().Int("id-read-workers", 1, "Number of id read worker goroutines to run")
	config.NumSecondaryIDReadWorkers = rootCmd.Flags().Int("secondary-id-read-workers", 1, "Number of secondary id read worker goroutines to run")
	config.NumAggregationWorkers = rootCmd.Flags().Int("aggregation-workers", 1, "Number of aggregation worker goroutines to run")
	config.NumUpdateWorkers = rootCmd.Flags().Int("update-workers", 1, "Number of update worker goroutines to run")
	config.StatTickSpeedMillis = rootCmd.Flags().Int("stat-tick-speed", 100, "Milliseconds between stat updates")
	config.PrimaryURI = rootCmd.PersistentFlags().StringP("primary", "p", "", "Primary cluster to connect to")
	rootCmd.MarkFlagRequired("primary")
	config.MetadataURI = rootCmd.Flags().StringP("metadata", "m", "", "Metadata cluster to store benchmark state, defaults to primary cluster")
	config.Reset = rootCmd.Flags().BoolP("reset", "r", false, "Reset clusters DBs before starting")
	config.Sharded = rootCmd.Flags().Bool("sharded", false, "Enable sharding on user_id")
}
