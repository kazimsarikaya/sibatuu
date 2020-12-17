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
	"errors"
	"fmt"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	prettytable "github.com/jedib0t/go-pretty/v6/table"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"io"
	klog "k8s.io/klog/v2"
	"os"
	"path/filepath"
	"syscall"
)

const (
	repoInfo string = "repoinfo"
)

type RepositoryHelper struct {
	Repository
	cache *Cache
	fs    backupfs.BackupFS
	ch    *ChunkHelper
	bh    *BackupHelper
}

func NewRepositoy(fs backupfs.BackupFS) (*RepositoryHelper, error) {
	sum := [32]byte{}
	rh := RepositoryHelper{}
	rh.CreateTime = ptypes.TimestampNow()
	rh.LastUpdated = ptypes.TimestampNow()
	rh.Checksum = sum[:]
	rh.fs = fs
	return &rh, nil
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

	rh := RepositoryHelper{}

	if err = proto.Unmarshal(data, &rh); err != nil {
		klog.V(0).Error(err, "cannot build repoinfo")
		return nil, err
	}

	sum := rh.GetChecksum()
	klog.V(6).Infof("checksum %v", sum)

	zerosum := [32]byte{}
	rh.Checksum = zerosum[:]
	postout, err := proto.Marshal(&rh)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return nil, err
	}
	testsum := sha256.Sum256(postout)
	klog.V(6).Infof("testsum %v", testsum)
	for i := range sum {
		if sum[i] != testsum[i] {
			klog.V(0).Error(err, "checksum mismatch")
			return nil, errors.New("checksum mismatch")
		}
	}
	rh.Checksum = sum[:]

	rh.fs = fs
	rh.ch, err = NewChunkHelper(fs, rh.GetLastChunkId()+1)
	if err != nil {
		klog.V(5).Error(err, "cannot create chunk helper")
		return nil, err
	}

	rh.bh, err = NewBackupHelper(rh.fs)
	if err != nil {
		klog.V(0).Error(err, "cannot create backup helper")
		return nil, err
	}

	cache, err := NewCache(fs, cacheDir, rh.ch, rh.bh)
	if err != nil {
		klog.V(0).Error(err, "error occured while creating cache")
		return nil, err
	}
	rh.cache = cache

	klog.V(5).Infof("repoinfo %v", rh)
	return &rh, nil
}

func (rh *RepositoryHelper) Initialize() error {
	if err := rh.fs.Mkdirs("."); err != nil {
		klog.V(0).Error(err, "cannot create repository folder")
		return err
	}

	if err := rh.fs.Mkdirs(chunksDir); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	if err := rh.fs.Mkdirs(backupsDir); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	if err := rh.writeData(); err != nil {
		klog.V(0).Error(err, "cannot init repository")
		return err
	}

	return nil
}

func (rh *RepositoryHelper) writeData() error {
	zerosum := [32]byte{}
	rh.Checksum = zerosum[:]
	klog.V(5).Infof("repo data %v", rh)
	preout, err := proto.Marshal(rh)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info")
		return err
	}
	klog.V(6).Infof("protobuf %v", preout)

	sum := sha256.Sum256(preout)
	rh.Checksum = sum[:]
	klog.V(5).Infof("sum %v", rh.Checksum)
	klog.V(5).Infof("repo data with new checksum %v", rh)

	out, err := proto.Marshal(rh)
	if err != nil {
		klog.V(0).Error(err, "cannot encode repository info with checksum")
		return err
	}
	klog.V(6).Infof("protobuf %v", out)

	writer, err := rh.fs.Create(repoInfo)
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

	klog.V(0).Infof("repo data writen")
	return nil
}

func (rh *RepositoryHelper) Backup(path, tag string) error {
	err := rh.ch.startChunkSession()
	if err != nil {
		klog.V(5).Error(err, "cannot start chunk session")
		return err
	}
	var bid = rh.GetLastBackupId() + 1
	ts, err := rh.bh.startBackupSession(bid, tag)
	if err != nil {
		klog.V(5).Error(err, "cannot start backup session")
		return err
	}

	var last_chunk_id uint64

	err = filepath.Walk(path, func(file string, info os.FileInfo, err error) error {
		file_sys := info.Sys()
		uid := file_sys.(*syscall.Stat_t).Uid
		gid := file_sys.(*syscall.Stat_t).Gid
		if info.Mode().IsRegular() { // normal file
			rh.bh.createFile(file[len(path):], info.ModTime(), info.Size(), info.Mode(), uid, gid)
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
				var chunk_id uint64
				chunk_id, res := rh.cache.findChunkId(sum[:])
				if !res {
					chunk_id, err = rh.ch.append(data, sum[:])
					last_chunk_id = chunk_id
					rh.cache.appendDirtyChunkId(chunk_id, sum[:])
					if err != nil {
						klog.V(0).Error(err, "cannot append chunk")
						return err
					}
				}
				rh.bh.addChunkIdToFile(chunk_id)
			}
			rh.bh.closeFile()
		} else if info.Mode()&os.ModeSymlink != 0 { // symlink
			rh.bh.createFile(file[len(path):], info.ModTime(), 0, info.Mode(), uid, gid)
			symt, _ := os.Readlink(file)
			rh.bh.setSymTarget(symt)
			rh.bh.closeFile()
		} else if info.Mode().IsDir() { // directory
			rh.bh.createFile(file[len(path):], info.ModTime(), 0, info.Mode(), uid, gid)
			rh.bh.closeFile()
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = rh.ch.endSession()
	if err != nil {
		klog.V(5).Error(err, "cannot end chunk helper")
	}

	err = rh.bh.endSession()
	if err != nil {
		klog.V(5).Error(err, "cannot end backup helper")
	}
	rh.LastBackupId = bid
	rh.LastChunkId = last_chunk_id
	rh.LastUpdated = ts

	err = rh.writeData()
	if err != nil {
		klog.V(5).Error(err, "cannot update repository info")
		return err
	}

	return nil
}

func (rh *RepositoryHelper) ListBackups() error {
	backups, err := rh.bh.getAllBackups()
	if err != nil {
		klog.V(5).Error(err, "cannot get all backups")
		return err
	}
	t := prettytable.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(prettytable.Row{"#", "Backup Time", "Tag", "Item Count", "chunk count", "Total Size"})
	var total_chunk_count float64 = 0
	for _, backup := range backups {
		var total_len uint64 = 0
		var chunk_count int = 0
		for _, fi := range backup.FileInfos {
			total_len += fi.FileLength
			chunk_count += len(fi.ChunkIds)
		}
		total_chunk_count += float64(chunk_count)
		t.AppendRow(prettytable.Row{backup.BackupId, ptypes.TimestampString(backup.BackupTime), backup.Tag, len(backup.FileInfos), chunk_count, total_len})
	}
	t.AppendFooter(prettytable.Row{"", "", "", "", "Repository Size", rh.cache.getTotalChunkSize()})
	dedup_ratio := 1 - float64(rh.cache.getChunkCount())/total_chunk_count
	t.AppendFooter(prettytable.Row{"", "", "", "", "Dedup Ratio", fmt.Sprintf("%.2f", dedup_ratio)})
	compress_ratio := 1 - float64(rh.cache.getTotalChunkSize())/(float64(rh.cache.getChunkCount())*float64(chunkSize))
	t.AppendFooter(prettytable.Row{"", "", "", "", "Compress Ratio", fmt.Sprintf("%.2f", compress_ratio)})
	t.Render()
	return nil
}

func (rh *RepositoryHelper) ListBackupWithId(bid uint64) {
	backup := rh.bh.getBackupById(bid)
	rh.listBackup(backup)
}

func (rh *RepositoryHelper) ListBackupWithTag(tag string) {
	backup := rh.bh.getBackupByTag(tag)
	rh.listBackup(backup)
}

func (rh *RepositoryHelper) listBackup(backup *Backup) {
	if backup == nil {
		klog.V(0).Infof("backup not found")
	}
	t := prettytable.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(prettytable.Row{"#", "Backup Time", "Tag", "Item Count", "chunk count", "Total Size"})
	var total_len uint64 = 0
	var chunk_count int = 0
	for _, fi := range backup.FileInfos {
		total_len += fi.FileLength
		chunk_count += len(fi.ChunkIds)
	}
	t.AppendRow(prettytable.Row{backup.BackupId, ptypes.TimestampString(backup.BackupTime), backup.Tag, len(backup.FileInfos), chunk_count, total_len})
	t.Render()

	t = prettytable.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(prettytable.Row{"#", "File Name", "Last Modified", "Owner", "Group", "Type", "Perms", "Chunk Count", "Length"})
	for fid, fi := range backup.FileInfos {
		mode := os.FileMode(fi.Mode)
		var item_type string = "F"
		if mode&os.ModeSymlink != 0 {
			item_type = "S"
		}
		if mode&os.ModeDir != 0 {
			item_type = "D"
		}
		t.AppendRow(prettytable.Row{fid, fi.FileName, ptypes.TimestampString(fi.LastModified), fi.Uid, fi.Gid, item_type, mode & os.ModePerm, len(fi.ChunkIds), fi.FileLength})
	}
	t.Render()
}
