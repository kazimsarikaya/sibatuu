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
	"github.com/kazimsarikaya/backup/internal/backupfs"
	"io"
	klog "k8s.io/klog/v2"
	"strconv"
)

const (
	chunkDir         string = "chunks"
	maxChunkBlobSize int64  = 128 << 20
	chunkSize        int64  = 4 << 10
)

type ChunkHelper struct {
	currentChunkBlob     string
	currentChunkBlobSize int64
	currentChunkWriter   io.WriteCloser
	fs                   backupfs.BackupFS
	nextChunkId          uint64
	appendSize           int64
	currentChunkInfos    *ChunkInfos
}

func NewChunkHelper(fs backupfs.BackupFS, nextChunkId uint64) (*ChunkHelper, error) {
	ch := &ChunkHelper{fs: fs, nextChunkId: nextChunkId}
	_, err := ch.getLastChunkBlobOrNew()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (ch *ChunkHelper) getAllChunks() ([]*ChunkInfo, error) {
	cfiles, err := ch.fs.List(chunkDir)
	if err != nil {
		return nil, err
	}
	var cinfos []*ChunkInfo
	for _, cfile := range cfiles {
		klog.V(5).Infof("chunk blob %v", cfile)
		inf, err := ch.fs.Open(chunkDir + "/" + cfile)
		if err != nil {
			klog.V(5).Infof("cannot open chunk blob %v", cfile)
			return nil, err
		}
		data := make([]byte, 16)
		inf.Seek(-16, 2)
		inf.Read(data)
		r, datalen := checkHeaderAndGetLength(data)
		if !r {
			klog.V(5).Infof("incorrect trailer for chunk blob %v %v", cfile, data)
			return nil, errors.New("cannot read chunk infos length")
		}
		pos, err := ch.fs.Length(chunkDir + "/" + cfile)
		if err != nil {
			return nil, err
		}
		pos += -16 - datalen
		for {
			data = make([]byte, datalen)
			inf.Seek(pos, 0)
			inf.Read(data)
			var chunk_infos ChunkInfos
			err := proto.Unmarshal(data, &chunk_infos)
			if err != nil {
				klog.V(5).Infof("cannot unmarshal chunkinfos at location %v length %v", pos, datalen)
				return nil, err
			}
			for _, chunk_info := range chunk_infos.GetChunkInfos() {
				cinfos = append(cinfos, chunk_info)
			}
			if chunk_infos.Previous.Length == 0 {
				break
			}
			pos = int64(chunk_infos.Previous.Start)
			datalen = int64(chunk_infos.Previous.Length)
		}
	}
	return cinfos, nil
}

func (ch *ChunkHelper) append(data, sum []byte) (uint64, error) {
	datalen := int64(len(data))
	var err error = nil
	if ch.currentChunkBlobSize+datalen > maxChunkBlobSize {
		err = ch.endSession()
		if err != nil {
			return 0, err
		}
		nname, err := getNewChunkBlobName(ch.currentChunkBlob)
		if err != nil {
			return 0, err
		}
		ch.currentChunkBlob = nname
		ch.currentChunkBlobSize = 0
		err = ch.startSession()
		if err != nil {
			return 0, err
		}
	}
	ci := &ChunkInfo{ChunkId: ch.nextChunkId, Start: uint64(ch.currentChunkBlobSize), Length: uint64(datalen), Checksum: sum}
	wl, err := ch.currentChunkWriter.Write(data)
	if err != nil || int64(wl) != datalen {
		if err == nil {
			err = errors.New("written data length mismatch")
		}
		return 0, err
	}
	ch.nextChunkId += 1
	ch.currentChunkInfos.ChunkInfos = append(ch.currentChunkInfos.ChunkInfos, ci)
	return ci.ChunkId, err
}

func (ch *ChunkHelper) endSession() error {
	if len(ch.currentChunkInfos.GetChunkInfos()) == 0 {
		ch.currentChunkWriter.Close()
		return nil
	}
	preout, err := proto.Marshal(ch.currentChunkInfos)
	if err != nil {
		klog.V(0).Error(err, "cannot encode chunk infos")
		return err
	}
	sum := sha256.Sum256(preout)
	ch.currentChunkInfos.Checksum = sum[:]
	klog.V(5).Infof("sum %v", ch.currentChunkInfos.Checksum)

	out, err := proto.Marshal(ch.currentChunkInfos)
	if err != nil {
		klog.V(0).Error(err, "cannot encode chunk infos with checksum")
		return err
	}
	writer := ch.currentChunkWriter
	_, err = writer.Write(out)
	if err != nil {
		klog.V(0).Error(err, "cannot write chunk infos")
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
	writer.Close()
	return nil
}

func (ch *ChunkHelper) startSession() error {
	prevChunkInfos := &ChunkInfos_PreviousChunkInfos{Start: 0, Length: 0}
	if ch.currentChunkBlobSize != 0 {
		reader, err := ch.fs.Open(chunkDir + "/" + ch.currentChunkBlob)
		if err != nil {
			klog.V(5).Error(err, "cannot find last chunk infos")
			return err
		}
		reader.Seek(-16, 2)
		data := make([]byte, 16)
		len, err := reader.Read(data)
		if len != 16 || err != nil {
			klog.V(5).Infof("cannot read last chunks infos length")
			if err == nil {
				err = errors.New("cannot read last chunks infos length")
			}
			return err
		}
		var datalen int64 = 0
		var r bool
		if r, datalen = checkHeaderAndGetLength(data); !r || datalen == 0 {
			klog.V(0).Infof("chunk infos broken")
			return errors.New("chunk infos broken")
		}
		if 16+datalen >= ch.currentChunkBlobSize {
			klog.V(5).Infof("chunk infos broken")
			return errors.New("chunk infos broken")
		}
		prevChunkInfos.Start = uint64(ch.currentChunkBlobSize - 16 - datalen)
		prevChunkInfos.Length = uint64(datalen)
		// TODO: need lookup checksum?
		reader.Close()
	}
	var cis []*ChunkInfo
	ch.currentChunkInfos = &ChunkInfos{Previous: prevChunkInfos, ChunkInfos: cis}
	w, err := ch.fs.Append(chunkDir + "/" + ch.currentChunkBlob)
	if err != nil {
		klog.V(5).Error(err, "cannot create appender")
		return err
	}
	ch.currentChunkWriter = w
	return nil
}

func (ch *ChunkHelper) getLastChunkBlobOrNew() (string, error) {
	chunks, err := ch.fs.List(chunkDir)
	if err != nil {
		klog.V(5).Error(err, "cannot list chunks folder")
		return "", err
	}
	var result string = ""
	if len(chunks) > 0 {
		result = chunks[len(chunks)-1]
	}
	if result != "" {
		fileSize, err := ch.fs.Length(chunkDir + "/" + result)
		if err != nil {
			return "", err
		}
		if fileSize < maxChunkBlobSize {
			ch.currentChunkBlob = result
			ch.currentChunkBlobSize = fileSize
			return result, nil
		}
	}
	result, err = getNewChunkBlobName(result)
	ch.currentChunkBlob = result
	ch.currentChunkBlobSize = 0
	if err != nil {
		klog.V(5).Error(err, "cannot generate chunk blob name")
		return "", err
	}
	return result, err
}

func getNewChunkBlobName(name string) (string, error) {
	if name == "" {
		return "000000000000", nil
	}
	id, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		return "", err
	}
	id += 1
	return fmt.Sprintf("%012d", id), nil
}
