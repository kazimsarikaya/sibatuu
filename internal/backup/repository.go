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

type RepositoryHelper struct {
	Repository
	cache *Cache
	fs    backupfs.BackupFS
	ch    *ChunkHelper
}

func NewRepositoy(fs backupfs.BackupFS) (*RepositoryHelper, error) {
	sum := [32]byte{}
	r := RepositoryHelper{}
	r.CreateTime = ptypes.TimestampNow()
	r.LastUpdated = ptypes.TimestampNow()
	r.Checksum = sum[:]
	r.fs = fs
	return &r, nil
}

func OpenRepositoy(fs backupfs.BackupFS, cacheDir string) (*RepositoryHelper, error) {
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
	var datalen int64 = 0
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

	r := RepositoryHelper{}

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

	r.fs = fs
	r.ch, err = NewChunkHelper(fs, r.GetLastChunkId()+1)
	if err != nil {
		klog.V(5).Error(err, "cannot create chunk helper")
		return nil, err
	}
	cache, err := NewCache(fs, cacheDir, r.ch)
	if err != nil {
		klog.V(0).Error(err, "error occured while creating cache")
		return nil, err
	}
	r.cache = cache

	klog.V(5).Infof("repoinfo %v", r)
	return &r, nil
}

func (r *RepositoryHelper) Initialize() error {
	if err := r.fs.Mkdirs("."); err != nil {
		klog.V(0).Error(err, "cannot create repository folder")
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

	writer, err := r.fs.Create(repoInfo)
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

	if err = r.fs.Mkdirs(chunkDir); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	if err = r.fs.Mkdirs("backups"); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	klog.V(0).Infof("repo created")
	return nil
}

func (r *RepositoryHelper) Backup(path string) error {
	err := r.ch.startSession()
	if err != nil {
		klog.V(5).Error(err, "cannot start chunk session")
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
				rcnt, err := inf.Read(data)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				data = data[:rcnt]
				sum := sha256.Sum256(data)
				_, res := r.cache.findChunkId(sum[:])
				if !res {
					chunk_id, err := r.ch.append(data, sum[:])
					r.cache.appendDirtyChunkId(chunk_id, sum[:])
					if err != nil {
						klog.V(0).Error(err, "cannot append chunk")
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = r.ch.endSession()
	if err != nil {
		klog.V(5).Error(err, "cannot end chunk helper")
	}
	return err
}
