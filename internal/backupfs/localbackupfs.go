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

package backupfs

import (
	"io"
	klog "k8s.io/klog/v2"
	"os"
	"path/filepath"
)

type LocalBackupFS struct {
	BackupFS
	basePath string
}

func NewLocalBackupFS(path string) (BackupFS, error) {
	fs := LocalBackupFS{basePath: path}
	return &fs, nil
}

func (fs *LocalBackupFS) Mkdirs(path string) error {
	return os.MkdirAll(fs.basePath+"/"+path, 0755)
}

func (fs *LocalBackupFS) Delete(path string) error {
	return os.RemoveAll(fs.basePath + "/" + path)
}

func (fs *LocalBackupFS) Create(path string) (io.WriteCloser, error) {
	return os.OpenFile(fs.basePath+"/"+path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
}

func (fs *LocalBackupFS) Open(path string) (ReadSeekCloser, error) {
	return os.OpenFile(fs.basePath+"/"+path, os.O_RDONLY, 0644)
}

func (fs *LocalBackupFS) Append(path string) (io.WriteCloser, error) {
	return os.OpenFile(fs.basePath+"/"+path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

func (fs *LocalBackupFS) List(path string) ([]string, error) {
	klog.V(5).Infof("listing folder %v", path)
	var files []string
	err := filepath.Walk(fs.basePath+"/"+path, func(file string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			file = file[len(fs.basePath+"/"+path):]
			klog.V(5).Infof("file found %v", file)
			files = append(files, file)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (fs *LocalBackupFS) Length(path string) (int64, error) {
	fi, err := os.Stat(fs.basePath + "/" + path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
