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
	klog "k8s.io/klog/v2"
)

const (
	repoInfo string = ".repoinfo"
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

	if !checkHeader(data) {
		klog.V(0).Error(err, "repoinfo meta broken")
		return nil, err
	}

	datalen := binary.LittleEndian.Uint64(data[8:])
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
		klog.V(0).Error(err, "cannot encode repository info witch checksum")
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

	klog.V(0).Infof("repo created")
	return nil
}
