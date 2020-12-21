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

package internal

import (
	"time"
)

const (
	RepoInfo           string        = "repoinfo"
	ChunksDir          string        = "chunks"
	BackupsDir         string        = "backups"
	LocksDir           string        = "locks"
	LocksControlDir    string        = "locks/control"
	LockFile           string        = "locks/lock"
	LockControlTimeout time.Duration = time.Second * 30
	MaxBlobSize        int64         = 128 << 20
	ChunkSize          int64         = 4 << 10
	IOBufferSize       int           = 128 << 10
	S3PartSize         int64         = 8 << 20
	S3ReaderBufferSize int           = 8 << 20
)

var (
	RepositoryHeader = []byte{0xFE, 0xED, 0xFA, 0xCE, 0xCA, 0xFE, 0xBE, 0xEF}
)
