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
	. "github.com/kazimsarikaya/backup/internal"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"io"
	klog "k8s.io/klog/v2"
	"os"
	"path/filepath"
	"syscall"
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
	reader, err := fs.Open(RepoInfo)
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
		return nil, errors.New("repoinfo meta broken")
	}

	reader.Seek(0, 0)
	data = make([]byte, datalen)
	len, err = reader.Read(data)
	if len != int(datalen) {
		klog.V(0).Error(err, "cannot read repoinfo")
		return nil, errors.New("cannot read repoinfo")
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
	rh.ch, err = NewChunkHelper(fs)
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

	if err := rh.fs.Mkdirs(ChunksDir); err != nil {
		klog.V(0).Error(err, "cannot create chunks folder")
		return err
	}

	if err := rh.fs.Mkdirs(BackupsDir); err != nil {
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

	writer, err := rh.fs.Create(RepoInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot create repoinfo")
		return err
	}
	defer writer.Close()

	_, err = writer.Write(out)
	if err != nil {
		klog.V(0).Error(err, "cannot write repoinfo")
		return writer.Abort()
	}

	if _, err = writer.Write(RepositoryHeader); err != nil {
		klog.V(0).Error(err, "cannot write trailer header")
		return writer.Abort()
	}
	lenarray := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenarray, uint64(len(out)))
	if _, err = writer.Write(lenarray); err != nil {
		klog.V(0).Error(err, "cannot write data len")
		return writer.Abort()
	}

	klog.V(0).Infof("repo data writen")
	return nil
}

func (rh *RepositoryHelper) Backup(path, tag string) error {
	rh.cache.getLastBackup(tag)
	last_chunk_id := rh.cache.getLastChunkId()
	err := rh.ch.startChunkSession(last_chunk_id + 1)
	if err != nil {
		klog.V(5).Error(err, "cannot start chunk session")
		return err
	}
	last_bid, err := rh.bh.GetLastBackupId()
	if err != nil {
		klog.V(5).Error(err, "cannot get last backup id")
		return err
	}
	bid := last_bid + 1
	ts, err := rh.bh.startBackupSession(bid, tag)
	if err != nil {
		klog.V(5).Error(err, "cannot start backup session")
		return err
	}

	err = filepath.Walk(path, func(file string, info os.FileInfo, err error) error {
		file_sys := info.Sys()
		uid := file_sys.(*syscall.Stat_t).Uid
		gid := file_sys.(*syscall.Stat_t).Gid
		trimmedPath := file[len(path):]
		if len(trimmedPath) > 0 {
			if trimmedPath[0] == '/' {
				trimmedPath = trimmedPath[1:]
			}
		}
		if trimmedPath == "" {
			trimmedPath = "."
		}
		klog.V(4).Infof("backup file %v", trimmedPath)
		if info.Mode().IsRegular() { // normal file
			rh.bh.createFile(trimmedPath, info.ModTime(), info.Size(), info.Mode(), uid, gid)
			changed, chunk_ids := rh.cache.isFileChangedOrGetChunkIds(trimmedPath, info)
			if !changed {
				for _, chunk_id := range chunk_ids {
					rh.bh.addChunkIdToFile(chunk_id)
				}
				klog.V(5).Infof("file %v not changed, added from cache", info.Name())
			} else {
				klog.V(5).Infof("file %v changed, backup started", info.Name())
				inf, err := os.Open(file)
				if err != nil {
					return err
				}
				for {
					data := make([]byte, ChunkSize)
					rcnt, err := inf.Read(data)
					if err != nil {
						if err == io.EOF {
							if rcnt <= 0 {
								break
							}
						} else {
							return err
						}
					}
					data = data[:rcnt]
					sum := sha256.Sum256(data)
					var chunk_id uint64
					chunk_id, res := rh.cache.findChunkId(sum[:])
					if !res {
						chunk_id, err = rh.ch.append(data, sum[:])
						if err != nil {
							klog.V(0).Error(err, "cannot append chunk")
							return err
						}
						last_chunk_id = chunk_id
						rh.cache.appendDirtyChunkId(chunk_id, sum[:])
					}
					rh.bh.addChunkIdToFile(chunk_id)
				}
				inf.Close()
				klog.V(5).Infof("backup of file %v ended", info.Name())
			}
			rh.bh.closeFile()
		} else if info.Mode()&os.ModeSymlink != 0 { // symlink
			rh.bh.createFile(trimmedPath, info.ModTime(), 0, info.Mode(), uid, gid)
			symt, _ := os.Readlink(file)
			rh.bh.setSymTarget(symt)
			rh.bh.closeFile()
		} else if info.Mode().IsDir() { // directory
			rh.bh.createFile(trimmedPath, info.ModTime(), 0, info.Mode(), uid, gid)
			rh.bh.closeFile()
		}
		return nil
	})
	if err != nil {
		rh.ch.abortSession()
		rh.bh.abortSession()
		return err
	}

	err = rh.ch.endSession()
	if err != nil {
		rh.bh.abortSession()
		klog.V(5).Error(err, "cannot end chunk helper")
		return err
	}

	err = rh.bh.endSession()
	if err != nil {
		klog.V(5).Error(err, "cannot end backup helper, please rebackup")
		return err
	}
	rh.LastUpdated = ts

	err = rh.writeData()
	if err != nil {
		klog.V(0).Error(err, "cannot update repository info, please run fix")
		return err
	}
	klog.V(4).Infof("backup finished. backup id %v backup tag %v", bid, tag)
	return nil
}

func (rh *RepositoryHelper) ListBackups() error {
	backups, err := rh.bh.getAllBackups()
	if err != nil {
		klog.V(5).Error(err, "cannot get all backups")
		return err
	}
	return rh.listBackups(backups)
}

func (rh *RepositoryHelper) ListBackupsWithTag(tag string) error {
	backups := rh.bh.getBackupsByTag(tag)
	return rh.listBackups(backups)
}

func (rh *RepositoryHelper) listBackups(backups []*Backup) error {
	t := prettytable.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(prettytable.Row{"#", "Backup Time", "Tag", "Item Count", "chunk count", "Total Size"})
	var total_chunk_count float64 = 0
	uniq_chunk_ids := make(map[uint64]struct{})
	var exists = struct{}{}
	for _, backup := range backups {
		var total_len uint64 = 0
		var chunk_count int = 0
		for _, fi := range backup.FileInfos {
			total_len += fi.FileLength
			chunk_count += len(fi.ChunkIds)
			for _, id := range fi.ChunkIds {
				uniq_chunk_ids[id] = exists
			}
		}
		total_chunk_count += float64(chunk_count)
		t.AppendRow(prettytable.Row{backup.BackupId, ptypes.TimestampString(backup.BackupTime), backup.Tag, len(backup.FileInfos), chunk_count, total_len})
	}
	t.AppendFooter(prettytable.Row{"", "", "", "", "Repository Size", rh.cache.getTotalChunkSize()})
	klog.V(6).Infof("total chunk count at backups %v uniq chunk count at backups %v", total_chunk_count, len(uniq_chunk_ids))
	dedup_ratio := 1 - float64(len(uniq_chunk_ids))/total_chunk_count
	t.AppendFooter(prettytable.Row{"", "", "", "", "Dedup Ratio", fmt.Sprintf("%.2f", dedup_ratio)})
	compress_ratio := 1 - float64(rh.cache.getTotalSizeOfChunks(uniq_chunk_ids))/(float64(rh.cache.getChunkCount())*float64(ChunkSize))
	t.AppendFooter(prettytable.Row{"", "", "", "", "Compress Ratio", fmt.Sprintf("%.2f", compress_ratio)})
	t.AppendFooter(prettytable.Row{"", "", "", "", "Last Chunk Id", rh.cache.getLastChunkId()})
	t.Render()
	return nil
}

func (rh *RepositoryHelper) ListBackupWithId(bid uint64) {
	backup := rh.bh.getBackupById(bid)
	rh.listBackup(backup)
}

func (rh *RepositoryHelper) ListLatestBackupWithFilteredByTag(tag string) {
	backup := rh.bh.getLatestBackupWithFilteredByTag(tag)
	rh.listBackup(backup)
}

func (rh *RepositoryHelper) listBackup(backup *Backup) {
	if backup == nil {
		klog.V(0).Error(errors.New("backup not found"), "cannot list backup content")
		return
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

func (rh *RepositoryHelper) RestoreItemsWithBid(destination string, bid uint64, override bool) error {
	backup := rh.bh.getBackupById(bid)
	if backup == nil {
		return errors.New("backup not found")
	}
	return rh.restoreItems(destination, backup, override)
}

func (rh *RepositoryHelper) RestoreItemWithFidWithBid(destination string, fid int, bid uint64, override bool) error {
	backup := rh.bh.getBackupById(bid)
	if backup == nil {
		return errors.New("backup not found")
	}
	fi := rh.getFileInfoWithFid(backup, fid)
	return rh.restoreItem(destination, fi, backup, override)
}

func (rh *RepositoryHelper) RestoreItemWithFnameWithBid(destination, fname string, bid uint64, override bool) error {
	backup := rh.bh.getBackupById(bid)
	if backup == nil {
		return errors.New("backup not found")
	}
	fi := rh.getFileInfoWithFname(backup, fname)
	return rh.restoreItem(destination, fi, backup, override)
}

func (rh *RepositoryHelper) RestoreLatestItemsFilteredWithBtag(destination, tag string, override bool) error {
	backup := rh.bh.getLatestBackupWithFilteredByTag(tag)
	if backup == nil {
		return errors.New("backup not found")
	}
	return rh.restoreItems(destination, backup, override)
}

func (rh *RepositoryHelper) RestoreLatestItemWithFidFilteredWithBtag(destination string, fid int, tag string, override bool) error {
	backup := rh.bh.getLatestBackupWithFilteredByTag(tag)
	if backup == nil {
		return errors.New("backup not found")
	}
	fi := rh.getFileInfoWithFid(backup, fid)
	return rh.restoreItem(destination, fi, backup, override)
}

func (rh *RepositoryHelper) RestoreLatestItemWithFnameFilteredWithBtag(destination, fname, tag string, override bool) error {
	backup := rh.bh.getLatestBackupWithFilteredByTag(tag)
	if backup == nil {
		return errors.New("backup not found")
	}
	fi := rh.getFileInfoWithFname(backup, fname)
	return rh.restoreItem(destination, fi, backup, override)
}

func (rh *RepositoryHelper) getFileInfoWithFid(backup *Backup, fid int) *Backup_FileInfo {
	if backup == nil {
		return nil
	}
	for id, fi := range backup.FileInfos {
		if id == fid {
			return fi
		}
	}
	return nil
}

func (rh *RepositoryHelper) getFileInfoWithFname(backup *Backup, fname string) *Backup_FileInfo {
	if backup == nil {
		return nil
	}
	for _, fi := range backup.FileInfos {
		if fi.FileName == fname {
			return fi
		}
	}
	return nil
}

func (rh *RepositoryHelper) restoreItems(destination string, backup *Backup, override bool) error {
	if backup == nil {
		return errors.New("backup not found")
	}
	for _, fi := range backup.FileInfos {
		if err := rh.restoreItem(destination, fi, backup, override); err != nil {
			return err
		}
	}
	return rh.fixMtimes(destination, backup)
}

func (rh *RepositoryHelper) fixMtimes(destination string, backup *Backup) error {
	if backup == nil {
		return errors.New("backup not found")
	}
	for _, fi := range backup.FileInfos {
		t, _ := ptypes.Timestamp(fi.LastModified)
		path2Fix := destination + "/" + fi.FileName
		os.Chtimes(path2Fix, t, t)
	}
	return nil
}

func (rh *RepositoryHelper) restoreItem(destination string, fi *Backup_FileInfo, backup *Backup, override bool) error {
	if fi == nil {
		return errors.New("file not found")
	}
	targetItem := fi.FileName
	mode := os.FileMode(fi.Mode)
	t, _ := ptypes.Timestamp(fi.LastModified)
	if targetItem == "." {
		if _, err := os.Stat(destination); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(destination, mode)
				os.Chtimes(destination, t, t)
				os.Chown(destination, int(fi.Uid), int(fi.Gid))
				return nil
			}
			klog.V(5).Error(err, "unknown error at stat %v", destination)
			return err
		}
		return nil
	}
	targetItemDir := filepath.Dir(targetItem)
	p_fi := rh.getFileInfoWithFname(backup, targetItemDir)
	if err := rh.restoreItem(destination, p_fi, backup, override); err != nil {
		klog.V(5).Error(err, "error creating parent dir "+targetItemDir)
		return err
	}
	path2C := destination + "/" + targetItem

	if _, err := os.Stat(path2C); err != nil {
		if os.IsNotExist(err) {
			if mode.IsRegular() {
				if err := rh.restoreFileData(path2C, fi); err != nil {
					klog.V(5).Error(err, "cannot restore file data "+path2C)
					return err
				}
			} else if mode&os.ModeSymlink != 0 {
				if err := os.Symlink(*fi.SymTargetFileName, path2C); err != nil {
					klog.V(5).Error(err, "cannot restore symlink "+path2C)
					return err
				}
			} else if mode.IsDir() {
				if err := os.MkdirAll(path2C, mode); err != nil {
					klog.V(5).Error(err, "cannot restore directory "+path2C)
					return err
				}
			} else {
				return errors.New("unknown file type at backup")
			}
			os.Chtimes(path2C, t, t)
			os.Chown(path2C, int(fi.Uid), int(fi.Gid))
			return nil
		}
		return err
	} else if override {
		if mode.IsDir() {
			return nil
		}
		if err := os.Remove(path2C); err != nil {
			return err
		}
		if mode.IsRegular() {
			if err := rh.restoreFileData(path2C, fi); err != nil {
				klog.V(5).Error(err, "cannot restore file data "+path2C)
				return err
			}
		} else if mode&os.ModeSymlink != 0 {
			if err := os.Symlink(*fi.SymTargetFileName, path2C); err != nil {
				klog.V(5).Error(err, "cannot restore symlink "+path2C)
				return err
			}
		} else {
			return errors.New("unknown file type at backup")
		}
		os.Chtimes(path2C, t, t)
		os.Chown(path2C, int(fi.Uid), int(fi.Gid))
	} else {
		if !mode.IsDir() {
			klog.V(5).Infof("destination item exists and not overriden %v", path2C)
		}
	}
	return nil
}

func (rh *RepositoryHelper) restoreFileData(dest string, fi *Backup_FileInfo) error {
	outf, err := os.Create(dest)
	if err != nil {
		klog.V(5).Error(err, "cannot create file "+dest)
		return err
	}
	for _, cid := range fi.ChunkIds {
		bf, start, len := rh.cache.getBlobFileOfChunkId(cid)
		if bf == nil {
			return errors.New(fmt.Sprintf("cannot find blob file for chunk id %v", cid))
		}
		klog.V(5).Infof("try to get chunk id %v for %v", cid, dest)
		data, err := rh.ch.getChunkData(*bf, start, len)
		if err != nil {
			klog.V(5).Error(err, fmt.Sprintf("chunk data not received for file %v for chunk id %v", dest, cid))
			return err
		}
		if data == nil {
			return errors.New(fmt.Sprintf("cannot get data for chunk id %v", cid))
		}
		outf.Write(data)
		klog.V(6).Infof("chunk id %v restored for %v", cid, dest)
	}
	outf.Close()
	return nil
}
