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
	"github.com/kazimsarikaya/backup/internal/backup"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	klog "k8s.io/klog/v2"
	"os"
	"path"
)

var (
	repository   string = ""
	source       string = ""
	cacheDir     string = ".cache"
	backupTag    string = ""
	initRepo     bool   = false
	listBackup   bool   = false
	createBackup bool   = false
	backupId     uint64 = 0

	showVersion = flag.Bool("version", false, "Show version.")

	version   = ""
	buildTime = ""
)

func init() {
	klog.InitFlags(nil)

	flag.StringVar(&repository, "r", ".", "Backup repository")
	flag.StringVar(&repository, "repo", ".", "Backup repository")

	flag.StringVar(&source, "s", "", "Source where backup taken from")
	flag.StringVar(&source, "source", "", "Source where backup taken from")

	flag.StringVar(&cacheDir, "c", ".cache", "Cache directory")
	flag.StringVar(&cacheDir, "cache", ".cache", "Cache directory")

	flag.StringVar(&backupTag, "t", "", "Backup Tag for backup/listing/restoring")
	flag.StringVar(&backupTag, "tag", "", "Backup Tag for backup/listing/restoring")
	flag.Uint64Var(&backupId, "bid", 0, "Backup Id for listing/restoring")
	flag.Uint64Var(&backupId, "backupid", 0, "Backup Id for listing/restoring")

	flag.BoolVar(&initRepo, "init", false, "Init repository")
	flag.BoolVar(&listBackup, "list", false, "List backups or given backup content")
	flag.BoolVar(&createBackup, "backup", false, "Create backup")

	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	if *showVersion {
		baseName := path.Base(os.Args[0])
		fmt.Println(baseName, version, buildTime)
		return
	}

	fs, err := backupfs.NewLocalBackupFS(repository)
	if err != nil {
		klog.V(0).Error(err, "cannot create file system for repo "+repository)
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
			klog.V(0).Infof("cannot have bot id and tag")
			os.Exit(1)
		}
		return
	}
}
