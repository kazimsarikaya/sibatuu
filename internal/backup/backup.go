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
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	klog "k8s.io/klog/v2"
	"os"
	"sort"
	"time"
)

type BackupHelper struct {
	Blob
	currentFile *Backup_FileInfo
}

func NewBackupHelper(fs backupfs.BackupFS) (*BackupHelper, error) {
	bh := &BackupHelper{}
	bh.fs = fs
	bh.blobsDir = backupsDir
	bh.getLastBlobOrNew()
	return bh, nil
}

func (bh *BackupHelper) startBackupSession(backupId uint64, tag string) error {
	err := bh.startSession(func() BlobInterface {
		return &Backup{BackupTime: ptypes.TimestampNow(), BackupId: backupId, Tag: tag}
	})
	return err
}

func (bh *BackupHelper) createFile(fileName string, mt time.Time, len int64, mode os.FileMode) {
	ts, _ := ptypes.TimestampProto(mt)
	bh.currentFile = &Backup_FileInfo{
		FileName:     fileName,
		LastModified: ts,
		FileLength:   uint64(len),
		Mode:         uint32(mode),
	}
}

func (bh *BackupHelper) closeFile() {
	if bh.currentFile != nil {
		bh.currentBlobInfo.Append(bh.currentFile)
	}
}

func (bh *BackupHelper) addChunkIdToFile(chunk_id uint64) {
	if bh.currentFile != nil {
		bh.currentFile.ChunkIds = append(bh.currentFile.ChunkIds, chunk_id)
	}
}

func (bh *BackupHelper) getAllBackups() ([]*Backup, error) {
	rawresult, err := bh.getAllBlobInfos(func(data []byte, pos, datalen int64) (BlobInterface, error) {
		var backup Backup
		err := proto.Unmarshal(data, &backup)
		if err != nil {
			klog.V(5).Infof("cannot unmarshal chunkinfos at location %v length %v", pos, datalen)
			return nil, err
		}
		return &backup, nil
	})
	if err != nil {
		return nil, err
	}
	result := make([]*Backup, len(rawresult))
	for i := range rawresult {
		result[i] = rawresult[i].(*Backup)
	}
	return result, nil
}

func (bh *BackupHelper) getLastBackup() (*Backup, error) {
	allBackups, err := bh.getAllBackups()
	if err != nil {
		klog.V(5).Error(err, "cannot get all backups")
	}
	sort.Slice(allBackups, func(i, j int) bool {
		return allBackups[i].GetBackupTime().Seconds < allBackups[j].GetBackupTime().Seconds
	})
	if len(allBackups) > 0 {
		return allBackups[len(allBackups)-1], nil
	}
	return nil, nil
}

func (b *Backup) IsEmpty() bool {
	return len(b.GetFileInfos()) == 0
}

func (b *Backup) SetChecksum(sum []byte) {
	b.Checksum = sum
}

func (b *Backup) SetPrevious(prev *Previous) {
	b.Previous = prev
}

func (b *Backup) Append(item interface{}) {
	b.FileInfos = append(b.FileInfos, item.(*Backup_FileInfo))
}
