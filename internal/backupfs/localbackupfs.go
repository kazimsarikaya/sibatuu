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
	. "github.com/kazimsarikaya/backup/internal"
	"io"
	klog "k8s.io/klog/v2"
	"os"
	"path/filepath"
)

type LocalBackupFS struct {
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

func (fs *LocalBackupFS) Create(path string) (WriteCloseAborter, error) {
	return NewLocalWriteCloseAborter(fs.basePath+"/"+path, false)
}

func (fs *LocalBackupFS) Open(path string) (ReadSeekCloser, error) {
	return os.OpenFile(fs.basePath+"/"+path, os.O_RDONLY, 0644)
}

func (fs *LocalBackupFS) Append(path string) (WriteCloseAborter, error) {
	return NewLocalWriteCloseAborter(fs.basePath+"/"+path, true)
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

type LocalWriteCloseAborter struct {
	path             string
	append           bool
	tempWriterCloser io.WriteCloser
}

func NewLocalWriteCloseAborter(path string, append bool) (WriteCloseAborter, error) {

	tmpWC, err := os.OpenFile(path+"-tmp", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &LocalWriteCloseAborter{path: path, append: append, tempWriterCloser: tmpWC}, nil
}

func (wca *LocalWriteCloseAborter) Write(data []byte) (int, error) {
	return wca.tempWriterCloser.Write(data)
}

func (wca *LocalWriteCloseAborter) Abort() error {
	err := wca.tempWriterCloser.Close()
	if err != nil {
		return err
	}
	err = os.Remove(wca.path + "-tmp")
	return err
}

func (wca *LocalWriteCloseAborter) Close() error {
	var err error
	err = wca.tempWriterCloser.Close()
	if err != nil {
		return err
	}

	var fi os.FileInfo
	var not_e bool = true
	var oldlen int64 = 0
	fi, err = os.Stat(wca.path)
	if err != nil {
		if !os.IsNotExist(err) {
			os.Remove(wca.path + "-tmp")
			return err
		}
	} else {
		oldlen = fi.Size()
		not_e = false
	}

	var w io.WriteCloser
	if wca.append {
		w, err = os.OpenFile(wca.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		w, err = os.OpenFile(wca.path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	}
	if err != nil {
		os.Remove(wca.path + "-tmp")
		return err
	}

	var r io.ReadCloser
	r, err = os.OpenFile(wca.path+"-tmp", os.O_RDONLY, 0644)
	if err != nil {
		os.Remove(wca.path + "-tmp")
		return err
	}

	var twc int64 = 0
	var wc, rc int = 0, 0
	for {
		data := make([]byte, IOBufferSize)
		rc, err = r.Read(data)
		if err != nil {
			if err == io.EOF {
				if rc > 0 {
					wc, err = w.Write(data[:rc])
					twc += int64(wc)
				}
				err = nil
			}
			break
		}
		if rc > 0 {
			wc, err = w.Write(data[:rc])
			twc += int64(wc)
			if err != nil {
				break
			}
		} else {
			break
		}
	}
	r.Close()
	w.Close()
	os.Remove(wca.path + "-tmp")
	if err != nil {
		klog.V(6).Error(err, "cannot write to the original destination")
		if !not_e {
			err = os.Truncate(wca.path, oldlen)
		}
	}
	return err
}
