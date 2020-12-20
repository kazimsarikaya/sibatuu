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

package listcmd

import (
	"errors"
	"github.com/kazimsarikaya/backup/internal/backup"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"github.com/spf13/cobra"
	klog "k8s.io/klog/v2"
)

var (
	listCmd = &cobra.Command{
		Use:   "list",
		Short: "List backups",
		Long: `List backups. If no parameters given list all backup information.
If latest parameter given, filters backups with tag as prefix and display latest backup content.
Also any full tag or backup id given for listing backup contents.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repository, err := cmd.Flags().GetString("repository")
			if err != nil {
				return err
			}
			cache, err := cmd.Flags().GetString("cache")
			if err != nil {
				return err
			}
			bid, err := cmd.Flags().GetUint64("backup-id")
			if err != nil {
				return err
			}
			tag, err := cmd.Flags().GetString("tag")
			if err != nil {
				return err
			}
			latest, err := cmd.Flags().GetBool("latest")
			if err != nil {
				return err
			}

			klog.V(5).Infof("list command called with repository %v cache %v backup id %v tag %v latest? %v", repository, cache, bid, tag, latest)
			fs, err := backupfs.GetBackupFS(repository)
			if err != nil {
				return err
			}
			r, err := backup.OpenRepositoy(fs, cache)
			if err != nil {
				return err
			}

			if latest {
				r.ListLatestBackupWithFilteredByTag(tag)
			} else {
				if bid == 0 && tag == "" {
					r.ListBackups()
				} else if bid != 0 && tag == "" {
					r.ListBackupWithId(bid)
				} else if bid == 0 && tag != "" {
					r.ListBackupsWithTag(tag)
				} else {
					return errors.New("cannot have both id and tag")
				}
			}
			return nil
		},
	}
)

func GetListCmd() *cobra.Command {
	listCmd.Flags().Uint64P("backup-id", "i", 0, "Backup id for listing given backup")
	listCmd.Flags().StringP("tag", "t", "", "Backup tag for listing given backup")
	listCmd.Flags().BoolP("latest", "l", false, "List latest backup. if tag given filter by tag prefix")
	return listCmd
}
