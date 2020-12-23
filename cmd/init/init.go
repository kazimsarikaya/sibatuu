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

package initcmd

import (
	"errors"
	"github.com/kazimsarikaya/sibatuu/internal/backup"
	"github.com/kazimsarikaya/sibatuu/internal/backupfs"
	"github.com/spf13/cobra"
	klog "k8s.io/klog/v2"
)

var (
	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize a backup repository",
		Long: `Initialze a backup repository with given path (-r)
repository path can be start with file: or s3:, if not given default is file.
Configuration can be given from config file, as parameter of envrionment variables.
The apply order is sames as description`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repository, err := cmd.Flags().GetString("repository")
			if err != nil {
				return err
			}
			if repository == "" {
				return errors.New("Empty repository parameter")
			}
			klog.V(5).Infof("init command called with repository %v", repository)
			fs, err := backupfs.GetBackupFS(repository)
			if err != nil {
				return err
			}
			r, _ := backup.NewRepositoy(fs)
			return r.Initialize()
		},
	}
)

func GetInitCmd() *cobra.Command {
	return initCmd
}
