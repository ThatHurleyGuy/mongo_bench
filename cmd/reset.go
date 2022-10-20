/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thathurleyguy/mongo_bench/bencher"
)

// resetCmd represents the reset command
var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Resets the dbs",
	Long:  `Drops all collections and indexes`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("reset called")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bencher := bencher.NewBencher(ctx, &config)
		if *config.MetadataURI == "" {
			config.MetadataURI = config.PrimaryURI
		}
		bencher.Reset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)
}
