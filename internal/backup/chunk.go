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
	"errors"
	proto "github.com/golang/protobuf/proto"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	klog "k8s.io/klog/v2"
)

type ChunkHelper struct {
	Blob
	nextChunkId uint64
}

func NewChunkHelper(fs backupfs.BackupFS, nextChunkId uint64) (*ChunkHelper, error) {
	ch := &ChunkHelper{}
	ch.fs = fs
	ch.nextChunkId = nextChunkId
	ch.blobsDir = chunksDir
	_, err := ch.getLastBlobOrNew()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (ch *ChunkHelper) getAllChunks() ([]*ChunkInfo, error) {
	var cinfos []*ChunkInfo
	_, err := ch.getAllBlobInfos(func(data []byte, pos, datalen int64) (BlobInterface, error) {
		var chunk_infos ChunkInfos
		err := proto.Unmarshal(data, &chunk_infos)
		if err != nil {
			klog.V(5).Infof("cannot unmarshal chunkinfos at location %v length %v", pos, datalen)
			return nil, err
		}
		cinfos = append(cinfos, chunk_infos.GetChunkInfos()...)
		return &chunk_infos, nil
	})
	if err != nil {
		return nil, err
	}
	return cinfos, nil
}

func (ch *ChunkHelper) append(chunk_data, sum []byte) (uint64, error) {
	var err error = nil
	data := encoder.EncodeAll(chunk_data, nil)
	datalen := int64(len(data))

	if ch.currentBlobSize+datalen > maxBlobSize {
		err = ch.endSession()
		if err != nil {
			return 0, err
		}
		nname, err := getNewBlobName(ch.currentBlob)
		if err != nil {
			return 0, err
		}
		ch.currentBlob = nname
		ch.currentBlobSize = 0
		err = ch.startSession(func() BlobInterface {
			var cis []*ChunkInfo
			return &ChunkInfos{ChunkInfos: cis}
		})
		if err != nil {
			return 0, err
		}
	}
	ci := &ChunkInfo{ChunkId: ch.nextChunkId, Start: uint64(ch.currentBlobSize), Length: uint64(datalen), Checksum: sum}
	wl, err := ch.currentWriter.Write(data)
	if err != nil || int64(wl) != datalen {
		if err == nil {
			err = errors.New("written data length mismatch")
		}
		return 0, err
	}
	ch.nextChunkId += 1
	ch.currentBlobInfo.Append(ci)
	return ci.ChunkId, err
}

func (ch *ChunkHelper) startChunkSession() error {
	return ch.startSession(func() BlobInterface {
		var cis []*ChunkInfo
		return &ChunkInfos{ChunkInfos: cis}
	})
}

func (ci *ChunkInfos) IsEmpty() bool {
	return len(ci.GetChunkInfos()) == 0
}

func (ci *ChunkInfos) SetChecksum(sum []byte) {
	ci.Checksum = sum
}

func (ci *ChunkInfos) SetPrevious(prev *Previous) {
	ci.Previous = prev
}

func (ci *ChunkInfos) Append(item interface{}) {
	ci.ChunkInfos = append(ci.ChunkInfos, item.(*ChunkInfo))
}
