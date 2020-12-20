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
	"errors"
	"flag"
	"fmt"
	"github.com/kazimsarikaya/backup/internal/backup"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	klog "k8s.io/klog/v2"
	"net/url"
	"os"
	"path"
)

var (
	repository    string = ""
	source        string = ""
	cacheDir      string = ".cache"
	backupTag     string = ""
	initRepo      bool   = false
	listBackup    bool   = false
	createBackup  bool   = false
	restoreBackup bool   = false
	backupId      uint64 = 0
	fileId        int    = -1
	fileName      string = ""
	destination   string = ""
	override      bool   = false

	showVersion = flag.Bool("version", false, "Show version.")

	version   = ""
	buildTime = ""
)

func init() {
	klog.InitFlags(nil)

	flag.StringVar(&repository, "r", ".", "Backup repository")
	flag.StringVar(&repository, "repo", ".", "Backup repository")

	flag.StringVar(&source, "src", "", "Source where backup taken from")
	flag.StringVar(&source, "source", "", "Source where backup taken from")

	flag.StringVar(&cacheDir, "c", ".cache", "Cache directory")
	flag.StringVar(&cacheDir, "cache", ".cache", "Cache directory")

	flag.StringVar(&backupTag, "t", "", "Backup Tag for backup/listing/restoring")
	flag.StringVar(&backupTag, "tag", "", "Backup Tag for backup/listing/restoring")
	flag.Uint64Var(&backupId, "bid", 0, "Backup Id for listing/restoring")
	flag.Uint64Var(&backupId, "backupid", 0, "Backup Id for listing/restoring")

	flag.StringVar(&fileName, "fn", "", "file name to restore single file")
	flag.StringVar(&fileName, "filename", "", "file name to restore single file")
	flag.IntVar(&fileId, "fid", -1, "file id to restore single file")
	flag.IntVar(&fileId, "fileid", -1, "file id to restore single file")

	flag.StringVar(&destination, "dst", "", "restoration target folder")
	flag.StringVar(&destination, "destination", "", "restoration target folder")

	flag.BoolVar(&initRepo, "init", false, "Init repository")
	flag.BoolVar(&listBackup, "list", false, "List backups or given backup content")
	flag.BoolVar(&createBackup, "backup", false, "Create backup")
	flag.BoolVar(&restoreBackup, "restore", false, "Restore backup or single item by id/name if given")

	flag.BoolVar(&override, "override", false, "Override if file exists at destination")

	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	if *showVersion {
		baseName := path.Base(os.Args[0])
		fmt.Println(baseName, version, buildTime)
		return
	}

	repoUrl, err := url.Parse(repository)

	var fs backupfs.BackupFS

	if repoUrl.Scheme == "" || repoUrl.Scheme == "file" {
		fs, err = backupfs.NewLocalBackupFS(repoUrl.Path)
		if err != nil {
			klog.V(0).Error(err, "cannot create file system for repo "+repository)
			os.Exit(1)
		}
	} else if repoUrl.Scheme == "s3" {
		fs, err = backupfs.NewS3BackupFS(repoUrl)
		if err != nil {
			klog.V(0).Error(err, "cannot create file system for repo "+repository)
			os.Exit(1)
		}
	} else {
		klog.V(0).Error(errors.New("unknown scheme"), "cannot create file system for repo "+repository)
		os.Exit(1)
	}

	if initRepo {
		r, _ := backup.NewRepositoy(fs)
		err := r.Initialize()
		if err != nil {
			klog.V(0).Error(err, "error occured while initialize repo "+repository)
			os.Exit(1)
		}
		return
	}
	if createBackup {
		r, err := backup.OpenRepositoy(fs, cacheDir)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
		err = r.Backup(source, backupTag)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
		return
	}
	if listBackup {
		r, err := backup.OpenRepositoy(fs, cacheDir)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
		if backupId == 0 && backupTag == "" {
			r.ListBackups()
		} else if backupId != 0 && backupTag == "" {
			r.ListBackupWithId(backupId)
		} else if backupId == 0 && backupTag != "" {
			r.ListBackupWithTag(backupTag)
		} else {
			klog.V(0).Infof("cannot have both id and tag")
			os.Exit(1)
		}
		return
	}
	if restoreBackup {
		if destination == "" {
			klog.V(0).Error(errors.New("no destination given"), "cannot restore")
			os.Exit(1)
		}
		r, err := backup.OpenRepositoy(fs, cacheDir)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
		if backupId != 0 && backupTag == "" {
			if fileId == -1 && fileName == "" {
				err = r.RestoreItemsWithBid(destination, backupId, override)
			} else if fileId != -1 && fileName == "" {
				err = r.RestoreItemWithFidWithBid(destination, fileId, backupId, override)
			} else if fileId == -1 && fileName != "" {
				err = r.RestoreItemWithFnameWithBid(destination, fileName, backupId, override)
			} else {
				klog.V(0).Infof("cannot have both file id and name")
				os.Exit(1)
			}
		} else if backupId == 0 && backupTag != "" {
			if fileId == -1 && fileName == "" {
				err = r.RestoreItemsWithBtag(destination, backupTag, override)
			} else if fileId != -1 && fileName == "" {
				err = r.RestoreItemWithFidWithBtag(destination, fileId, backupTag, override)
			} else if fileId == -1 && fileName != "" {
				err = r.RestoreItemWithFnameWithBtag(destination, fileName, backupTag, override)
			} else {
				klog.V(0).Infof("cannot have both file id and name")
				os.Exit(1)
			}
		} else {
			klog.V(0).Infof("one of backup id or tag required")
			os.Exit(1)
		}
		if err != nil {
			klog.V(0).Error(err, "cannot restore")
		}
	}
}
