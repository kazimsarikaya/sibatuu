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

package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	klog "k8s.io/klog/v2"
	"os"
	"time"
)

const (
	repoInfo string = ".repoinfo"
)

var (
	repositoryHeader = [...]byte{0xFE, 0xED, 0xFA, 0xCE, 0xCA, 0xFE, 0xBE, 0xEF}
)

type Repository struct {
	Header     [8]byte
	CreateTime int64
	Checksum   [32]byte
}

func NewRepositoy() (*Repository, error) {
	r := Repository{Header: repositoryHeader, CreateTime: time.Now().UnixNano()}
	return &r, nil
}

func (r *Repository) Initialize(path string) error {
	var preBuffer bytes.Buffer
	enc := gob.NewEncoder(&preBuffer)
	err := enc.Encode(r)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return err
	}

	sum := sha256.Sum256(preBuffer.Bytes())
	copy(r.Checksum[:], sum[:])

	var buffer bytes.Buffer
	enc = gob.NewEncoder(&buffer)
	err = enc.Encode(r)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return err
	}

	f, err := os.Create(path + repoInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot create repoinfo")
		return err
	}
	defer f.Close()

	_, err = f.Write(buffer.Bytes())
	if err != nil {
		klog.V(0).Error(err, "cannot write repoinfo")
		return err
	}
	return nil
}
