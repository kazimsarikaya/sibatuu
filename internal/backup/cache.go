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
	proto "github.com/golang/protobuf/proto"
	"github.com/kazimsarikaya/backup/internal/backupfs"
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
	for _, ci := range c.LocalCache.GetChunkInfos() {
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
	cinfos, err := c.ch.getAllChunks()
	if err != nil {
		klog.V(5).Error(err, "cannot get all chunks")
		return err
	}
	backup, err := c.bh.getLastBackup()
	if err != nil {
		klog.V(5).Error(err, "cannot get last backup")
		return err
	}

	c.LocalCache = &LocalCache{ChunkInfos: cinfos, LastBackup: backup}

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
	os.MkdirAll(c.cacheDir, 0700)
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
