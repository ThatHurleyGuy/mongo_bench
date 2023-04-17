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
	config.NumWorkers = rootCmd.Flags().IntP("num-workers", "n", 10, "Number of worker goroutines to run")
	config.WorkerReadWriteRatio = rootCmd.Flags().IntP("worker-read-write-ratio", "w", 1, "Ratio of read to write workers")
	config.StatTickSpeedMillis = rootCmd.Flags().Int("stat-tick-speed", 100, "Milliseconds between stat updates")
	config.PrimaryURI = rootCmd.PersistentFlags().StringP("primary", "p", "", "Primary cluster to connect to")
	config.ReaderURI = rootCmd.PersistentFlags().StringP("reader", "s", "", "Reader URI connection")
	config.MetadataURI = rootCmd.PersistentFlags().StringP("metadata", "m", "", "Mongo cluster for metadata storage")
	rootCmd.MarkFlagRequired("primary")
	config.Reset = rootCmd.Flags().BoolP("reset", "r", false, "Reset clusters DBs before starting")
	config.Sharded = rootCmd.Flags().Bool("sharded", false, "Enable sharding on user_id")
}
