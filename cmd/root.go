package cmd

import (
	"github.com/Cameron-Kurotori/microservices-coding-challenge/cmd/clientcmd"
	"github.com/Cameron-Kurotori/microservices-coding-challenge/cmd/servercmd"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "microservices-coding-challenge",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	rootCmd.AddCommand(clientcmd.ClientCmd)
	rootCmd.AddCommand(servercmd.ServerCmd)
	rootCmd.CompletionOptions.HiddenDefaultCmd = true
}

func Execute() error {
	return rootCmd.Execute()
}
