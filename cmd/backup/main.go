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
	"time"
)

var (
	repository string = ""
	source     string = ""
	cacheDir   string = ".cache"
	backupTag  string = ""
	initRepo   bool   = false

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

	flag.StringVar(&backupTag, "t", "", "Backup Tag")
	flag.StringVar(&backupTag, "tag", "", "Backup Tag")

	flag.BoolVar(&initRepo, "init", false, "Init repository")

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
	} else {
		r, err := backup.OpenRepositoy(fs, cacheDir)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
		if backupTag == "" {
			t := time.Now()
			backupTag = t.Format(time.RFC3339)
		}
		err = r.Backup(source, backupTag)
		if err != nil {
			klog.V(0).Error(err, "error occured while opening repo "+repository)
			os.Exit(1)
		}
	}
}
