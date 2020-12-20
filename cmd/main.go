/*
Copyright 2020 Kazım SARIKAYA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"github.com/kazimsarikaya/backup/cmd/backup"
	"github.com/kazimsarikaya/backup/cmd/init"
	"github.com/kazimsarikaya/backup/cmd/list"
	"github.com/kazimsarikaya/backup/cmd/restore"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	klog "k8s.io/klog/v2"
)

var (
	rootCmd = &cobra.Command{
		Use:   "backup",
		Short: "Backup/Restore tool",
		Long:  `A beatiful backup and restore tool supporting local file systems and s3`,
	}
	version   = ""
	buildTime = ""
	goVersion = ""

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Backup/Restore tool\n")
			fmt.Printf("Version: %v\n", version)
			fmt.Printf("Build Time: %v\n", buildTime)
			fmt.Printf("%v\n", goVersion)
		},
	}
)

func init() {

	klog.InitFlags(nil)

	rootCmd.PersistentFlags().StringP("repository", "r", "", "backup repository")
	rootCmd.PersistentFlags().StringP("cache", "c", "", "local cache directory")
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("logtostderr"))
	pflag.CommandLine.Set("logtostderr", "true")

	rootCmd.AddCommand(initcmd.GetInitCmd())
	rootCmd.AddCommand(backupcmd.GetBackupCmd())
	rootCmd.AddCommand(listcmd.GetListCmd())
	rootCmd.AddCommand(restorecmd.GetRestoreCmd())
	rootCmd.AddCommand(versionCmd)

}

func Execute() error {
	return rootCmd.Execute()
}

func main() {
	if err := Execute(); err != nil {
		klog.Errorf("backup command failed err=%v", err)
	}
}
