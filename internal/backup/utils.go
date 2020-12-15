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
	"encoding/binary"
)

var (
	repositoryHeader = []byte{0xFE, 0xED, 0xFA, 0xCE, 0xCA, 0xFE, 0xBE, 0xEF}
)

func checkHeaderAndGetLength(data []byte) (bool, uint64) {
	for i := range repositoryHeader {
		if repositoryHeader[i] != data[i] {
			return false, 0
		}
	}
	datalen := binary.LittleEndian.Uint64(data[8:])
	return true, datalen
}
