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

package backupcmd

import (
	"github.com/kazimsarikaya/backup/internal/backup"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"github.com/spf13/cobra"
	klog "k8s.io/klog/v2"
)

var (
	backupCmd = &cobra.Command{
		Use:   "backup",
		Short: "Backups given path to the repository",
		Long: `Backups given path, as parameter source, to the repository.
Command uses a local cache directory for metadata. Also a tag can be given.
If tag not given it's default is backup timestamp.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repository, err := cmd.Flags().GetString("repository")
			if err != nil {
				return err
			}
			source, err := cmd.Flags().GetString("source")
			if err != nil {
				return err
			}
			cache, err := cmd.Flags().GetString("cache")
			if err != nil {
				return err
			}
			tag, err := cmd.Flags().GetString("tag")
			if err != nil {
				return err
			}
			klog.V(5).Infof("backup command called with repository %v source %v cache %v tag %v", repository, source, cache, tag)
			fs, err := backupfs.GetBackupFS(repository)
			if err != nil {
				return err
			}
			r, err := backup.OpenRepositoy(fs, cache)
			if err != nil {
				return err
			}
			return r.Backup(source, tag)
		},
	}
)

func GetBackupCmd() *cobra.Command {
	backupCmd.Flags().StringP("source", "s", "", "Backup source which will be backuped")
	backupCmd.Flags().StringP("tag", "t", "", "Backup tag, if not given backup timestamp")
	return backupCmd
}
