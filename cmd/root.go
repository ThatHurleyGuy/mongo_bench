/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/thathurleyguy/mongo_bench/bencher"
)

// rootCmd represents the base command when called without any subcommands
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
	config.NumAggregationWorkers = rootCmd.Flags().Int("aggregation-works", 1, "Number of aggregation worker goroutines to run")
	config.NumUpdateWorkers = rootCmd.Flags().Int("update-workers", 1, "Number of update worker goroutines to run")
	config.StatTickSpeedMillis = rootCmd.Flags().Int("stat-tick-speed", 100, "Milliseconds between stat updates")
	config.Database = rootCmd.Flags().StringP("database", "d", "mongo_bench", "Database to store collection in")
	config.Collection = rootCmd.Flags().StringP("collection", "c", "transactions", "Collection name to store documents in")
	config.PrimaryURI = rootCmd.Flags().StringP("primary", "p", "", "Primary cluster to connect to")
	rootCmd.MarkFlagRequired("primary")
	config.SecondaryURI = rootCmd.Flags().StringP("secondary", "s", "", "Secondary cluster to connect to. Used to test dual reads in mongobetween")
	config.MetadataURI = rootCmd.Flags().StringP("metadata", "m", "", "Metadata cluster to store benchmark state, defaults to primary cluster")
}
