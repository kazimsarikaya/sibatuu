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
)

type BackupFS interface {
	Mkdirs(path string) error
	Delete(path string) error
	Create(path string) (WriteCloseAborter, error)
	Open(path string) (ReadSeekCloser, error)
	Append(path string) (WriteCloseAborter, error)
	List(path string) ([]string, error)
	Length(path string) (int64, error)
}

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type Aborter interface {
	Abort() error
}

type WriteCloseAborter interface {
	io.Writer
	io.Closer
	Aborter
}
