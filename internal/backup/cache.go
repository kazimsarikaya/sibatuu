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
	"errors"
	"fmt"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/kazimsarikaya/sibatuu/internal/backupfs"
	"io/ioutil"
	klog "k8s.io/klog/v2"
	"os"
)

const (
	localCaheFile string = "localCache"
)

type dirtyChunkId struct {
	chunkId uint64
	sum     []byte
}

type Cache struct {
	*LocalCache
	fs            backupfs.BackupFS
	cacheDir      string
	dirtyChunkIds []dirtyChunkId
	ch            *ChunkHelper
	bh            *BackupHelper
	lastBackup    *Backup
	chunkRefMap   map[uint64]int
}

func NewCache(fs backupfs.BackupFS, cacheDir string, ch *ChunkHelper, bh *BackupHelper) (*Cache, error) {
	c := &Cache{fs: fs, cacheDir: cacheDir, ch: ch, bh: bh}
	err := c.fillCache()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cache) appendDirtyChunkId(chunkId uint64, sum []byte) {
	dii := dirtyChunkId{chunkId: chunkId, sum: sum}
	c.dirtyChunkIds = append(c.dirtyChunkIds, dii)
}

func (c *Cache) findChunkId(sum []byte) (uint64, bool) {
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		for _, ci := range cifm.ChunkInfos {
			var found = true
			for i := range sum {
				if ci.Checksum[i] != sum[i] {
					found = false
				}
			}
			if found {
				return ci.GetChunkId(), true
			}
		}
	}

	for _, dii := range c.dirtyChunkIds {
		var found = true
		for i := range sum {
			if dii.sum[i] != sum[i] {
				found = false
			}
		}
		if found {
			return dii.chunkId, true
		}
	}
	return 0, false
}

func (c *Cache) fillCache() error {
	cinfos, err := c.ch.getAllChunkInfos()
	if err != nil {
		klog.V(5).Error(err, "cannot get all chunks")
		return err
	}

	var items []*LocalCache_ChunkInfoFileMap
	for key, value := range cinfos {
		items = append(items, &LocalCache_ChunkInfoFileMap{
			ChunkFile:  key,
			ChunkInfos: value,
		})
	}

	c.LocalCache = &LocalCache{ChunkInfoFileMaps: items}

	preout, err := proto.Marshal(c.LocalCache)
	if err != nil {
		klog.V(5).Error(err, "cannot encode local cache")
		return err
	}
	sum := sha256.Sum256(preout)
	c.LocalCache.Checksum = sum[:]
	objout, err := proto.Marshal(c.LocalCache)
	if err != nil {
		klog.V(5).Error(err, "cannot encode local cache")
		return err
	}
	err = os.RemoveAll(c.cacheDir)
	os.MkdirAll(c.cacheDir, 0700)
	os.MkdirAll(c.cacheDir+"/chunks", 0700)
	outf, err := os.Create(c.cacheDir + "/" + localCaheFile)
	if err != nil {
		klog.V(5).Error(err, "cannot create local cache file")
		return err
	}

	out := encoder.EncodeAll(objout, nil)
	_, err = outf.Write(out)
	if err != nil {
		klog.V(5).Error(err, "cannot create local cache file")
		return err
	}
	outf.Close()
	return nil
}

func (c *Cache) getChunkCount() int {
	var total_count int = 0
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		total_count += len(cifm.ChunkInfos)
	}
	klog.V(6).Infof("total chunk count %v", total_count)
	return total_count
}

func (c *Cache) getSizeOfChunk(chunk_id uint64) uint64 {
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		for _, ci := range cifm.ChunkInfos {
			if ci.ChunkId == chunk_id {
				return ci.Length
			}
		}
	}
	return 0
}

func (c *Cache) getTotalSizeOfChunks(chunk_ids map[uint64]struct{}) uint64 {
	var total_size uint64 = 0
	for chunk_id := range chunk_ids {
		total_size += c.getSizeOfChunk(chunk_id)
	}
	return total_size
}

func (c *Cache) getTotalChunkSize() uint64 {
	var total_size uint64 = 0
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		for _, ci := range cifm.ChunkInfos {
			total_size += ci.Length
		}
	}
	return total_size
}

func (c *Cache) getLastBackup(tag string) {
	c.lastBackup = c.bh.getLatestBackupWithFilteredByTag(tag)
	c.fillChunkRefMap()
}

func (c *Cache) setLastBackup(backup *Backup) {
	c.lastBackup = backup
	c.fillChunkRefMap()
}

func (c *Cache) fillChunkRefMap() {
	if c.lastBackup != nil {
		c.chunkRefMap = make(map[uint64]int)
		for _, fi := range c.lastBackup.FileInfos {
			for _, cid := range fi.ChunkIds {
				if _, ok := c.chunkRefMap[cid]; !ok {
					c.chunkRefMap[cid] = 1
				} else {
					c.chunkRefMap[cid] += 1
				}
			}
		}
	}
}

func (c *Cache) getChunkData(cid uint64) ([]byte, error) {
	_, err := os.Stat(fmt.Sprintf("%v/chunks/%v", c.cacheDir, cid))
	if os.IsNotExist(err) {
		bf, start, len := c.getBlobFileOfChunkId(cid)
		if bf == nil {
			return nil, errors.New(fmt.Sprintf("cannot find blob file for chunk id %v", cid))
		}
		klog.V(5).Infof("try to get chunk id %v", cid)
		data, err := c.ch.getChunkData(*bf, start, len)
		if err != nil {
			klog.V(5).Error(err, fmt.Sprintf("chunk data not received for chunk id %v", cid))
			return nil, err
		}
		if data == nil {
			return nil, errors.New(fmt.Sprintf("cannot get data for chunk id %v", cid))
		}
		if c.chunkRefMap[cid] > 1 {
			err = ioutil.WriteFile(fmt.Sprintf("%v/chunks/%v", c.cacheDir, cid), data, 0644)
			if err != nil {
				return nil, err
			}
		}
		return data, nil
	} else if err == nil {
		return ioutil.ReadFile(fmt.Sprintf("%v/chunks/%v", c.cacheDir, cid))
	} else {
		return nil, err
	}
}

func (c *Cache) isFileChangedOrGetChunkIds(trimmedPath string, info os.FileInfo) (bool, []uint64) {
	if c.lastBackup == nil {
		klog.V(6).Infof("any backup found, not returning old chunk ids")
		return true, nil
	}
	for _, fi := range c.lastBackup.FileInfos {
		if fi.FileName == trimmedPath {
			klog.V(6).Infof("file %v found at cache, checking it", trimmedPath)
			ts, _ := ptypes.TimestampProto(info.ModTime())
			if fi.LastModified.Seconds == ts.Seconds && fi.LastModified.Nanos == ts.Nanos && fi.FileLength == uint64(info.Size()) {
				klog.V(6).Infof("file %v not changed, returning its' chunk ids from cache", trimmedPath)
				return false, fi.ChunkIds
			}
			klog.V(6).Infof("file %v changed, not returning old chunk ids", trimmedPath)
			return true, nil
		}
	}
	klog.V(6).Infof("file %v not found at cache, not returning old chunk ids", trimmedPath)
	return true, nil
}

func (c *Cache) getBlobFileOfChunkId(chunk_id uint64) (*string, uint64, uint64) {
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		for _, ci := range cifm.ChunkInfos {
			if ci.ChunkId == chunk_id {
				return &cifm.ChunkFile, ci.Start, ci.Length
			}
		}
	}
	return nil, 0, 0
}

func (c *Cache) getLastChunkId() uint64 {
	var cid uint64 = 0
	for _, cifm := range c.LocalCache.GetChunkInfoFileMaps() {
		for _, ci := range cifm.ChunkInfos {
			if cid < ci.ChunkId {
				cid = ci.ChunkId
			}
		}
	}
	return cid
}
