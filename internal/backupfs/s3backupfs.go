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

package backupfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	. "github.com/kazimsarikaya/backup/internal"
	minio "github.com/minio/minio-go/v7"
	miniocred "github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"io/ioutil"
	klog "k8s.io/klog/v2"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type S3BackupFS struct {
	endpoint string
	bucket   string
	client   *minio.Client
	basePath string
}

func NewS3BackupFS(url *url.URL) (BackupFS, error) {
	var err error = nil
	pwd, _ := url.User.Password()
	p := url.Path
	if len(p) < 2 {
		return nil, errors.New("malformed s3 url")
	}
	if p[0] != '/' {
		return nil, errors.New("malformed s3 url")
	}
	var bucket, basePath string = "", "/"
	idx := strings.Index(p[1:], "/")
	if idx != -1 {
		bucket = p[1 : idx+1]
		basePath = p[idx+1:]
	} else {
		bucket = p[1:]
	}

	secure := false

	str_secure := url.Query().Get("secure")
	if str_secure != "" {
		if secure, err = strconv.ParseBool(str_secure); err != nil {
			return nil, err
		}
	}

	client, err := minio.New(url.Host, &minio.Options{
		Creds:  miniocred.NewStaticV4(url.User.Username(), pwd, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}

	found, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("bucket does not exist")
	}

	klog.V(5).Infof("s3 repository endpoint %v username %v bucket %v base path %v", url.Host, url.User.Username(), bucket, basePath)
	return &S3BackupFS{
		endpoint: url.Host,
		bucket:   bucket,
		client:   client,
		basePath: basePath,
	}, nil
}

func (fs *S3BackupFS) Mkdirs(path string) error {
	err := fs.ensureBasePath(fs.basePath)
	if err != nil {
		return err
	}
	path = filepath.Clean(path)
	if path == "." {
		return nil
	}
	parentDir := filepath.Dir(path)
	err = fs.Mkdirs(parentDir)
	if err != nil {
		return err
	}
	key := filepath.Clean(fs.basePath + "/" + path)
	if key[0] == '/' {
		key = key[1:]
	}
	if key[len(key)-1] != '/' {
		key += "/"
	}
	data := make([]byte, 0)
	r := bytes.NewReader(data)
	_, err = fs.client.PutObject(context.Background(), fs.bucket, key, r, 0, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (fs *S3BackupFS) ensureBasePath(path string) error {
	key := filepath.Clean(path)
	if key == "." || key == "/" {
		return nil
	} else {
		parentKey := filepath.Dir(key)
		err := fs.ensureBasePath(parentKey)
		if err != nil {
			return err
		}
	}
	if key[0] == '/' {
		key = key[1:]
	}
	if key[len(key)-1] != '/' {
		key += "/"
	}
	data := make([]byte, 0)
	r := bytes.NewReader(data)
	_, err := fs.client.PutObject(context.Background(), fs.bucket, key, r, 0, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (fs *S3BackupFS) Delete(path string) error {
	key := filepath.Clean(fs.basePath + "/" + path)

	if key[0] == '/' {
		key = key[1:]
	}

	klog.V(5).Infof("deleting items with prefix %v", key)

	objectsCh := make(chan minio.ObjectInfo)
	var errs []error

	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		for object := range fs.client.ListObjects(context.Background(), fs.bucket, minio.ListObjectsOptions{
			Prefix:    key,
			Recursive: true,
		}) {
			if object.Err != nil {
				klog.V(5).Error(object.Err, "cannot list objects")
				errs = append(errs, object.Err)
			}
			objectsCh <- object
		}
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}

	for rErr := range fs.client.RemoveObjects(context.Background(), fs.bucket, objectsCh, opts) {
		klog.V(5).Error(rErr.Err, "cannot delete object")
		errs = append(errs, rErr.Err)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New("multiple errors occured")
}

func (fs *S3BackupFS) Create(path string) (WriteCloseAborter, error) {
	return NewS3WriteCloseAborter(fs, path, false)
}

func (fs *S3BackupFS) Append(path string) (WriteCloseAborter, error) {
	return NewS3WriteCloseAborter(fs, path, true)
}

func (fs *S3BackupFS) Open(path string) (ReadSeekCloser, error) {
	return NewS3ReadSeekCloser(fs, path)
}

func (fs *S3BackupFS) List(path string) ([]string, error) {
	key := filepath.Clean(fs.basePath + "/" + path)

	if key[len(key)-1] != '/' {
		key += "/"
	}

	if key[0] == '/' {
		key = key[1:]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objectCh := fs.client.ListObjects(ctx, fs.bucket, minio.ListObjectsOptions{
		Prefix:    key,
		Recursive: false,
	})

	var res []string

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		res = append(res, obj.Key[len(key):])
	}

	return res, nil
}

func (fs *S3BackupFS) Length(path string) (int64, error) {
	key := filepath.Clean(fs.basePath + "/" + path)
	objInfo, err := fs.client.StatObject(context.Background(), fs.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return -1, err
	}
	return objInfo.Size, nil
}

type S3ReadSeekCloser struct {
	fs       *S3BackupFS
	path     string
	position int64
	size     int64
}

func NewS3ReadSeekCloser(fs *S3BackupFS, path string) (ReadSeekCloser, error) {
	size, err := fs.Length(path)
	if err != nil {
		return nil, err
	}
	klog.V(5).Infof("openning file %v at bucket %v file len %v", path, fs.bucket, size)
	return &S3ReadSeekCloser{fs: fs, path: path, position: 0, size: size}, nil
}

func (r *S3ReadSeekCloser) Read(data []byte) (int, error) {
	path := r.fs.basePath + "/" + r.path
	key := filepath.Clean(path)
	if key[0] == '/' {
		key = key[1:]
	}
	klog.V(5).Infof("try read file %v at position %v with len %v", key, r.position, len(data))
	object, err := r.fs.client.GetObject(context.Background(), r.fs.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return -1, err
	}

	if r.position >= r.size {
		return 0, io.EOF
	}
	_, err = object.Seek(r.position, 0)
	if err != nil {
		klog.V(5).Error(err, "error at seek")
		return -1, err
	}
	rc, err := object.Read(data)
	object.Close()
	if err != nil {
		if err.Error() != "EOF" {
			klog.V(5).Error(err, "error at read")
			return -1, err
		}
	}
	r.position += int64(rc)
	klog.V(5).Infof("read succeed for file %v at position %v with len %v", key, r.position, rc)
	return rc, nil
}

func (r *S3ReadSeekCloser) Seek(position int64, whence int) (int64, error) {
	newpos := r.position
	if whence == 0 {
		newpos = position
	} else if whence == 1 {
		newpos += position
	} else if whence == 2 {
		newpos = r.size + position
	} else {
		return r.position, errors.New("unknown whence")
	}
	if newpos < 0 || newpos > r.size {
		return r.position, errors.New(fmt.Sprintf("position out of file size boundaries: %v", newpos))
	}
	r.position = newpos
	return newpos, nil
}

func (r *S3ReadSeekCloser) Close() error {
	return nil
}

type S3WriteCloseAborter struct {
	fs              *S3BackupFS
	path            string
	append          bool
	partCount       int
	currentPartSize int64
	backendFile     string
	backendWriter   io.WriteCloser
	backendClosed   bool
}

func NewS3WriteCloseAborter(fs *S3BackupFS, path string, append bool) (WriteCloseAborter, error) {
	path = filepath.Clean(path)
	rappend := append
	if append {
		len, err := fs.Length(path)
		if err != nil {
			if err.Error() != "The specified key does not exist." {
				return nil, err
			}
		} else if len < S3PartSize {
			rappend = false
		}
	}
	w := &S3WriteCloseAborter{fs: fs, path: path, append: rappend}
	err := w.createBackend()
	if err != nil {
		w.Abort()
		return nil, err
	}
	if append && !rappend {
		r, err := fs.Open(path)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(w, r)
		if err != nil {
			return nil, err
		}
	}
	err = w.createPartsDir()
	if err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

func (w *S3WriteCloseAborter) Abort() error {
	w.backendWriter.Close()
	err := os.RemoveAll(w.backendFile)
	if err != nil {
		klog.V(5).Error(err, "cannot temporary backend file")
	}
	err = w.cleanup()
	if err != nil {
		klog.V(5).Error(err, "cannot cleanup s3 temp data")
	}
	return errors.New("writer aborted")
}

func (w *S3WriteCloseAborter) Close() error {
	if !w.backendClosed {
		err := w.backendWriter.Close()
		if err != nil {
			klog.V(5).Error(err, "cannot close backend writer aborting")
			return w.Abort()
		}
		w.backendClosed = true
	}

	err := w.uploadPart()
	if err != nil {
		klog.V(5).Error(err, "cannot upload part, aborting")
		return w.Abort()
	}

	err = w.mergeTemp()
	if err != nil {
		klog.V(5).Error(err, "cannot merge, aborting")
		return w.Abort()
	}

	err = w.replace()
	if err != nil {
		klog.V(5).Error(err, "cannot merge, aborting")
		return w.Abort()
	}

	return w.cleanup()
}

func (w *S3WriteCloseAborter) Write(data []byte) (int, error) {
	if w.currentPartSize >= S3PartSize {
		err := w.uploadPart()
		if err != nil {
			klog.V(5).Error(err, "cannot upload part, aborting")
			return -1, w.Abort()
		}
		err = w.createBackend()
		if err != nil {
			klog.V(5).Error(err, "cannot create backend, aborting")
			return -1, w.Abort()
		}
	}
	wc, err := w.backendWriter.Write(data)
	if err != nil {
		klog.V(5).Error(err, "cannot write backend, aborting")
		return -1, w.Abort()
	}
	w.currentPartSize += int64(wc)
	return wc, nil
}

func (w *S3WriteCloseAborter) uploadPart() error {
	if w.currentPartSize == 0 {
		return nil
	}
	if !w.backendClosed {
		err := w.backendWriter.Close()
		if err != nil {
			return err
		}
		w.backendClosed = true
	}
	file, err := os.Open(w.backendFile)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	partsDir := w.getPartsDir()
	part := fmt.Sprintf("%v/%v/part-%v", w.fs.basePath, partsDir, w.partCount+1)
	part = filepath.Clean(part)
	if part[0] == '/' {
		part = part[1:]
	}
	_, err = w.fs.client.PutObject(context.Background(),
		w.fs.bucket,
		part,
		file,
		fileStat.Size(),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return err
	}
	w.partCount += 1
	return nil
}

func (w *S3WriteCloseAborter) mergeTemp() error {
	var srcs []minio.CopySrcOptions

	if w.append {
		_, err := w.fs.Length(w.path)
		if err != nil {
			if err.Error() != "The specified key does not exist." {
				return err
			}
		} else {
			key := fmt.Sprintf("%v/%v", w.fs.basePath, w.path)
			key = filepath.Clean(key)
			if key[0] == '/' {
				key = key[1:]
			}
			klog.V(5).Infof("appending key %v", key)
			srcs = append(srcs, minio.CopySrcOptions{
				Bucket: w.fs.bucket,
				Object: key,
			})
		}
	}

	idx := 0
	for idx < w.partCount {
		key := fmt.Sprintf("%v/%v/part-%v", w.fs.basePath, w.getPartsDir(), idx+1)
		key = filepath.Clean(key)
		if key[0] == '/' {
			key = key[1:]
		}
		klog.V(5).Infof("appending key %v", key)
		srcs = append(srcs, minio.CopySrcOptions{
			Bucket: w.fs.bucket,
			Object: key,
		})
		idx += 1
	}

	dstKey := fmt.Sprintf("%v/%v/dst", w.fs.basePath, w.getPartsDir())
	dstKey = filepath.Clean(dstKey)
	if dstKey[0] == '/' {
		dstKey = dstKey[1:]
	}
	dst := minio.CopyDestOptions{
		Bucket: w.fs.bucket,
		Object: dstKey,
	}

	klog.V(6).Infof("merge sources %v", srcs)

	// Compose object call by concatenating multiple source files.
	_, err := w.fs.client.ComposeObject(context.Background(), dst, srcs...)
	if err != nil {
		klog.V(5).Error(err, "cannot merge to temporary location")
		return err
	}
	klog.V(5).Infof("merge to %v succeed", dstKey)
	return nil
}

func (w *S3WriteCloseAborter) replace() error {
	srcKey := fmt.Sprintf("%v/%v/dst", w.fs.basePath, w.getPartsDir())
	srcKey = filepath.Clean(srcKey)
	if srcKey[0] == '/' {
		srcKey = srcKey[1:]
	}
	src := minio.CopySrcOptions{
		Bucket: w.fs.bucket,
		Object: srcKey,
	}

	dstKey := fmt.Sprintf("%v/%v", w.fs.basePath, w.path)
	dstKey = filepath.Clean(dstKey)
	if dstKey[0] == '/' {
		dstKey = dstKey[1:]
	}
	dst := minio.CopyDestOptions{
		Bucket: w.fs.bucket,
		Object: dstKey,
	}

	// Copy object call
	_, err := w.fs.client.CopyObject(context.Background(), dst, src)
	if err != nil {
		klog.V(5).Error(err, "cannot replace destination")
		return err
	}
	return nil
}

func (w *S3WriteCloseAborter) createBackend() error {
	base := filepath.Base(w.path)
	file, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("%v-part-%v", base, w.partCount+1))
	if err != nil {
		klog.V(5).Error(err, "cannot create backend file")
		return err
	}
	w.backendFile = file.Name()
	w.backendWriter = file
	w.backendClosed = false
	w.currentPartSize = 0
	return nil
}

func (w *S3WriteCloseAborter) cleanup() error {
	partsDir := w.getPartsDir()
	klog.V(5).Infof("deleting parts dir %v", partsDir)
	err := w.fs.Delete(partsDir)
	if err != nil {
		klog.V(5).Error(err, "cleanup failed")
		return err
	}
	return nil
}

func (w *S3WriteCloseAborter) createPartsDir() error {
	partsDir := w.getPartsDir()
	klog.V(5).Infof("creating parts dir %v", partsDir)
	err := w.fs.Mkdirs(partsDir)
	if err != nil {
		klog.V(5).Error(err, "creating parts dir failed")
	}
	return err
}

func (w *S3WriteCloseAborter) getPartsDir() string {
	parentKey := filepath.Dir(w.path)
	base := filepath.Base(w.path)
	partsDir := parentKey + "/" + "parts-" + base
	return partsDir
}
