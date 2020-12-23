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

package restorecmd

import (
	"errors"
	"github.com/kazimsarikaya/sibatuu/internal/backup"
	"github.com/kazimsarikaya/sibatuu/internal/backupfs"
	"github.com/spf13/cobra"
	klog "k8s.io/klog/v2"
)

var (
	restoreCmd = &cobra.Command{
		Use:   "restore",
		Short: "Restore full backup or single file",
		Long: `Restores full backup or single file.
If only backup id or backup tag given, restores full backup.
If file id or file name given, restores single backup.
If latest parameter given, filters backup with tag as prefix and restore operation performed on latest of filtered backups.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repository, err := cmd.Flags().GetString("repository")
			if err != nil {
				return err
			}
			destination, err := cmd.Flags().GetString("destination")
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
			fid, err := cmd.Flags().GetInt("file-id")
			if err != nil {
				return err
			}
			fn, err := cmd.Flags().GetString("file-name")
			if err != nil {
				return err
			}
			override, err := cmd.Flags().GetBool("override")
			if err != nil {
				return err
			}

			if repository == "" {
				return errors.New("Empty repository parameter")
			}
			if destination == "" {
				return errors.New("Empty destination parameter")
			}

			klog.V(5).Infof("restore command called with repository %v cache %v backup id %v tag %v", repository, cache, bid, tag)
			fs, err := backupfs.GetBackupFS(repository)
			if err != nil {
				return err
			}
			r, err := backup.OpenRepositoy(fs, cache)
			if err != nil {
				return err
			}

			if bid == 0 && tag != "" {
				if fid == -1 && fn == "" {
					err = r.RestoreLatestItemsFilteredWithBtag(destination, tag, override)
				} else if fid != -1 && fn == "" {
					err = r.RestoreLatestItemWithFidFilteredWithBtag(destination, fid, tag, override)
				} else if fid == -1 && fn != "" {
					err = r.RestoreLatestItemWithFnameFilteredWithBtag(destination, fn, tag, override)
				} else {
					return errors.New("cannot have both file id and name")
				}
			} else if bid != 0 && tag == "" {
				if fid == -1 && fn == "" {
					err = r.RestoreItemsWithBid(destination, bid, override)
				} else if fid != -1 && fn == "" {
					err = r.RestoreItemWithFidWithBid(destination, fid, bid, override)
				} else if fid == -1 && fn != "" {
					err = r.RestoreItemWithFnameWithBid(destination, fn, bid, override)
				} else {
					return errors.New("cannot have both file id and name")
				}
			} else {
				return errors.New("one of backup id or tag required")
			}
			return err
		},
	}
)

func GetRestoreCmd() *cobra.Command {
	restoreCmd.Flags().StringP("destination", "d", "", "Backup restore destination directory")
	restoreCmd.Flags().Uint64P("backup-id", "i", 0, "Backup id for restoring given backup")
	restoreCmd.Flags().StringP("tag", "t", "", "Backup tag for listing given backup")
	restoreCmd.Flags().IntP("file-id", "f", -1, "File id for restore single file")
	restoreCmd.Flags().StringP("file-name", "n", "", "File name for restore single file")
	restoreCmd.Flags().BoolP("override", "o", false, "Override if file exists at destination")
	return restoreCmd
}
