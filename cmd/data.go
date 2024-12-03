// nolint
package cmd

import (
	"github.com/scalog/scalog/data"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// dataCmd represents the data command
var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "The data dissemination layer",
	Long:  `The data dissemination layer`,
	Run: func(cmd *cobra.Command, args []string) {
		data.Start()
	},
}

func init() {
	RootCmd.AddCommand(dataCmd)
	dataCmd.PersistentFlags().IntP("sid", "s", 0, "Shard index")
	dataCmd.PersistentFlags().IntP("rid", "r", 0, "Replica index in the shard")
	dataCmd.PersistentFlags().Float64P("rate", "t", 10000, "Rate of data generation in the emulation")
	dataCmd.PersistentFlags().Int64P("num_shards", "n", 1, "Number of shards")
	viper.BindPFlag("sid", dataCmd.PersistentFlags().Lookup("sid"))
	viper.BindPFlag("rid", dataCmd.PersistentFlags().Lookup("rid"))
	viper.BindPFlag("rate", dataCmd.PersistentFlags().Lookup("rate"))
	viper.BindPFlag("num_shards", dataCmd.PersistentFlags().Lookup("num_shards"))
}
