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
	"crypto/sha256"
	"encoding/binary"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"io"
	klog "k8s.io/klog/v2"
	"os"
	"path/filepath"
)

const (
	repoInfo string = "repoinfo"
)

func NewRepositoy() (*Repository, error) {
	sum := [32]byte{}
	r := Repository{CreateTime: ptypes.TimestampNow(), LastUpdated: ptypes.TimestampNow(), Checksum: sum[:]}
	return &r, nil
}

func OpenRepositoy(fs backupfs.BackupFS) (*Repository, error) {
	reader, err := fs.Open(repoInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot open repoinfo")
		return nil, err
	}
	defer reader.Close()

	_, err = reader.Seek(-16, 2)
	if err != nil {
		klog.V(0).Error(err, "cannot go end of repoinfo")
		return nil, err
	}
	data := make([]byte, 16)
	len, err := reader.Read(data)
	if len != 16 {
		klog.V(0).Error(err, "cannot read repoinfo meta")
		return nil, err
	}
	var datalen uint64 = 0
	var res bool
	if res, datalen = checkHeaderAndGetLength(data); !res {
		klog.V(0).Error(err, "repoinfo meta broken")
		return nil, err
	}

	reader.Seek(0, 0)
	data = make([]byte, datalen)
	len, err = reader.Read(data)
	if len != int(datalen) {
		klog.V(0).Error(err, "cannot read repoinfo")
		return nil, err
	}

	var r Repository

	if err = proto.Unmarshal(data, &r); err != nil {
		klog.V(0).Error(err, "cannot build repoinfo")
		return nil, err
	}

	sum := r.GetChecksum()

	zerosum := [32]byte{}
	r.Checksum = zerosum[:]
	postout, err := proto.Marshal(&r)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return nil, err
	}
	testsum := sha256.Sum256(postout)
	for i := range sum {
		if sum[i] != testsum[i] {
			klog.V(0).Error(err, "checksum mismatch")
			return nil, err
		}
	}
	r.Checksum = sum[:]

	klog.V(5).Infof("repoinfo %v", r)
	return &r, nil
}

func (r *Repository) Initialize(fs backupfs.BackupFS) error {
	if err := fs.Mkdirs("."); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	preout, err := proto.Marshal(r)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return err
	}
	klog.V(5).Infof("protobuf %v", preout)

	sum := sha256.Sum256(preout)
	r.Checksum = sum[:]
	klog.V(5).Infof("sum %v", r.Checksum)

	out, err := proto.Marshal(r)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info with checksum")
		return err
	}
	klog.V(5).Infof("protobuf %v", out)

	writer, err := fs.Create(repoInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot create repoinfo")
		return err
	}
	defer writer.Close()

	_, err = writer.Write(out)
	if err != nil {
		klog.V(0).Error(err, "cannot write repoinfo")
		return err
	}

	if _, err = writer.Write(repositoryHeader); err != nil {
		klog.V(0).Error(err, "cannot write trailer header")
		return err
	}
	lenarray := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenarray, uint64(len(out)))
	if _, err = writer.Write(lenarray); err != nil {
		klog.V(0).Error(err, "cannot write data len")
		return err
	}

	if err = fs.Mkdirs(chunkDir); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	if err = fs.Mkdirs("backups"); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	klog.V(0).Infof("repo created")
	return nil
}

func (r *Repository) Backup(fs backupfs.BackupFS, path string) error {
	ch, err := NewChunkHelper(fs, r.GetLastChunkId()+1)
	if err != nil {
		klog.V(5).Error(err, "cannot create chunk helper")
		return err
	}
	err = filepath.Walk(path, func(file string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			inf, err := os.Open(file)
			if err != nil {
				return err
			}
			for {
				data := make([]byte, chunkSize)
				r, err := inf.Read(data)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				data = data[:r]
				err = ch.append(data)
				if err != nil {
					klog.V(0).Error(err, "cannot append data")
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = ch.endSession()
	if err != nil {
		klog.V(5).Error(err, "cannot end chunk helper")
	}
	return err
}
