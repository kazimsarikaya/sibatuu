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
	"google.golang.org/protobuf/runtime/protoiface"
	"io"
	klog "k8s.io/klog/v2"
	"strconv"
)

const (
	chunksDir   string = "chunks"
	backupsDir  string = "backups"
	maxBlobSize int64  = 128 << 20
	chunkSize   int64  = 4 << 10
)

type BlobInterface interface {
	protoiface.MessageV1
	GetPrevious() *Previous
	SetPrevious(prev *Previous)
	IsEmpty() bool
	SetChecksum(sum []byte)
	GetChecksum() []byte
	Append(item interface{})
}

type Blob struct {
	blobsDir        string
	currentBlob     string
	currentBlobSize int64
	currentWriter   io.WriteCloser
	fs              backupfs.BackupFS
	currentBlobInfo BlobInterface
}

type blobParser func(data []byte, pos, datalen int64) (BlobInterface, error)
type blobCreator func() BlobInterface

func (b *Blob) getAllBlobInfos(parser blobParser) ([]BlobInterface, error) {
	blob_files, err := b.fs.List(b.blobsDir)
	if err != nil {
		return nil, err
	}
	var blobs []BlobInterface
	for _, blob_file := range blob_files {
		klog.V(5).Infof("blob %v", blob_file)
		inf, err := b.fs.Open(b.blobsDir + "/" + blob_file)
		if err != nil {
			klog.V(5).Infof("cannot open blob %v", blob_file)
			return nil, err
		}
		data := make([]byte, 16)
		inf.Seek(-16, 2)
		inf.Read(data)
		r, datalen := checkHeaderAndGetLength(data)
		if !r {
			klog.V(5).Infof("incorrect trailer for blob %v %v", blob_file, data)
			return nil, errors.New("cannot read blob info length")
		}
		pos, err := b.fs.Length(b.blobsDir + "/" + blob_file)
		if err != nil {
			return nil, err
		}
		pos += -16 - datalen
		for {
			data = make([]byte, datalen)
			inf.Seek(pos, 0)
			inf.Read(data)
			parsed, err := parser(data, pos, datalen)
			if err != nil {
				klog.V(5).Error(err, "cannot parse data")
				return nil, err
			}
			blobs = append(blobs, parsed)

			if parsed.GetPrevious().Length == 0 {
				break
			}
			pos = int64(parsed.GetPrevious().Start)
			datalen = int64(parsed.GetPrevious().Length)
		}
	}
	return blobs, nil
}

func (b *Blob) getLastBlobOrNew() (string, error) {
	blobs, err := b.fs.List(b.blobsDir)
	if err != nil {
		klog.V(5).Error(err, "cannot list blobs folder "+b.blobsDir)
		return "", err
	}
	var result string = ""
	if len(blobs) > 0 {
		result = blobs[len(blobs)-1]
	}
	if result != "" {
		fileSize, err := b.fs.Length(b.blobsDir + "/" + result)
		if err != nil {
			return "", err
		}
		if fileSize < maxBlobSize {
			b.currentBlob = result
			b.currentBlobSize = fileSize
			return result, nil
		}
	}
	result, err = getNewBlobName(result)
	b.currentBlob = result
	b.currentBlobSize = 0
	if err != nil {
		klog.V(5).Error(err, "cannot generate blob name")
		return "", err
	}
	return result, err
}

func getNewBlobName(name string) (string, error) {
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

func (b *Blob) startSession(creator blobCreator) error {
	prev := &Previous{Start: 0, Length: 0}
	if b.currentBlobSize != 0 {
		reader, err := b.fs.Open(b.blobsDir + "/" + b.currentBlob)
		if err != nil {
			klog.V(5).Error(err, "cannot find last chunk infos")
			return err
		}
		reader.Seek(-16, 2)
		data := make([]byte, 16)
		len, err := reader.Read(data)
		if len != 16 || err != nil {
			klog.V(5).Infof("cannot read last blob info length")
			if err == nil {
				err = errors.New("cannot read last blob info length")
			}
			return err
		}
		var datalen int64 = 0
		var r bool
		if r, datalen = checkHeaderAndGetLength(data); !r || datalen == 0 {
			klog.V(5).Infof("blob infos broken")
			return errors.New("blob infos broken")
		}
		if 16+datalen > b.currentBlobSize {
			klog.V(5).Infof("blob info broken ")
			return errors.New("blob info broken")
		}
		prev.Start = uint64(b.currentBlobSize - 16 - datalen)
		prev.Length = uint64(datalen)
		// TODO: need lookup checksum?
		reader.Close()
	}
	b.currentBlobInfo = creator()
	b.currentBlobInfo.SetPrevious(prev)
	w, err := b.fs.Append(b.blobsDir + "/" + b.currentBlob)
	if err != nil {
		klog.V(5).Error(err, "cannot create appender")
		return err
	}
	b.currentWriter = w
	return nil
}

func (b *Blob) endSession() error {
	if b.currentBlobInfo.IsEmpty() {
		b.currentWriter.Close()
		return nil
	}
	preout, err := proto.Marshal(b.currentBlobInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot encode chunk infos")
		return err
	}
	sum := sha256.Sum256(preout)
	b.currentBlobInfo.SetChecksum(sum[:])
	klog.V(5).Infof("sum %v", b.currentBlobInfo.GetChecksum())

	out, err := proto.Marshal(b.currentBlobInfo)
	if err != nil {
		klog.V(0).Error(err, "cannot encode chunk infos with checksum")
		return err
	}
	writer := b.currentWriter
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
