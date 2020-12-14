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
	klog "k8s.io/klog/v2"
	"os"
	"path"
)

var (
	repository = flag.String("r", ".", "Backup repository")
	initRepo   = flag.Bool("init", false, "Init repository")

	showVersion = flag.Bool("version", false, "Show version.")

	version   = ""
	buildTime = ""
)

func init() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()

	if *showVersion {
		baseName := path.Base(os.Args[0])
		fmt.Println(baseName, version, buildTime)
		return
	}

	if *initRepo {
		r, _ := backup.NewRepositoy()
		err := r.Initialize(*repository)
		if err != nil {
			klog.V(0).Error(err, "error occured while initialize repo %v %v", repository)
			os.Exit(1)
		}
	}

}
