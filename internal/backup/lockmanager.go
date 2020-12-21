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
	"github.com/google/uuid"
	. "github.com/kazimsarikaya/backup/internal"
	"github.com/kazimsarikaya/backup/internal/backupfs"
	klog "k8s.io/klog/v2"
	"time"
)

type LockManager struct {
	fs        backupfs.BackupFS
	lockerId  uuid.UUID
	abortFlag bool
}

func NewLockManager(fs backupfs.BackupFS) (*LockManager, error) {
	lockerId, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return &LockManager{fs: fs, lockerId: lockerId, abortFlag: false}, nil
}

func (lm *LockManager) abort() {
	lm.abortFlag = true
}

func (lm *LockManager) acquireLock() (bool, error) {
	lockIdStr := lm.lockerId.String()
	lockData, err := lm.lockerId.MarshalBinary()
	if err != nil {
		klog.V(5).Error(err, "cannot generate lock id data")
		return false, err
	}

	err = lm.fs.Mkdirs(LocksControlDir)
	if err != nil {
		klog.V(5).Error(err, "cannot create lock control dir if not exists")
		return false, err
	}

	lockControlFile := LocksControlDir + "/" + lockIdStr

	for !lm.abortFlag {
		w, err := lm.fs.Create(lockControlFile)
		if err != nil {
			klog.V(5).Error(err, "cannot create lock control file")
			return false, err
		}
		now := time.Now()
		timeData, err := now.MarshalBinary()
		if err != nil {
			klog.V(5).Error(err, "cannot generate timestamp data")
			return false, err
		}
		_, err = w.Write(timeData)
		if err != nil {
			klog.V(5).Error(err, "cannot write lock id data")
			return false, err
		}
		err = w.Close()
		if err != nil {
			klog.V(5).Error(err, "cannot write lock id data")
			return false, err
		}
		klog.V(5).Infof("i put myself into lockers directory")

		lockers, err := lm.fs.List(LocksControlDir)
		if err != nil {
			lm.fs.Delete(lockControlFile)
			klog.V(5).Error(err, "cannot list lockers")
			return false, err
		}
		if lockers[0] != lockIdStr {
			klog.V(5).Infof("i am not the first one at queue, check timeout of it")
			r, err := lm.fs.Open(LocksControlDir + "/" + lockers[0])
			if err == nil {
				lockerTimeData := make([]byte, 128)
				rc, err := r.Read(lockerTimeData)
				if err == nil {
					var lockerTime time.Time
					if lockerTime.UnmarshalBinary(lockerTimeData[:rc]) == nil {
						if time.Now().Sub(lockerTime) > LockControlTimeout {
							klog.V(5).Infof("first locker control file timeout, delete it")
							lm.fs.Delete(LocksControlDir + "/" + lockers[0])
						}
					}
				}
				r.Close()
			}
			klog.V(5).Infof("i am not the first one at queue sleeping 1 second")
			time.Sleep(time.Second)
			continue
		}
		klog.V(5).Infof("i have perm to check lock file")
		len, err := lm.fs.Length(LockFile)
		if err != nil {
			lm.fs.Delete(lockControlFile)
			klog.V(5).Error(err, "cannot get lock file size")
			return false, err
		}
		if len > 0 {
			klog.V(5).Infof("lock detected sleeping 1 second")
			time.Sleep(time.Second)
		} else {
			klog.V(5).Infof("i can gather lock")
			break
		}
	}
	if lm.abortFlag {
		lm.fs.Delete(lockControlFile)
		return false, errors.New("acquire lock aborted")
	}

	w, err := lm.fs.Create(LockFile)
	if err != nil {
		lm.fs.Delete(lockControlFile)
		klog.V(5).Error(err, "cannot create lock file")
		return false, err
	}
	_, err = w.Write(lockData)
	if err != nil {
		lm.fs.Delete(lockControlFile)
		klog.V(5).Error(err, "cannot write lock id data to lock file ")
		return false, err
	}
	err = w.Close()
	if err != nil {
		lm.fs.Delete(lockControlFile)
		klog.V(5).Error(err, "cannot write lock id data")
		return false, err
	}
	lm.fs.Delete(lockControlFile)
	klog.V(5).Infof("i gathered lock")
	return true, nil
}

func (lm *LockManager) releaseLock() (bool, error) {
	emptyData := make([]byte, 0)
	w, err := lm.fs.Create(LockFile)
	if err != nil {
		klog.V(5).Error(err, "cannot create empty lock file")
		return false, err
	}
	w.Write(emptyData)
	err = w.Close()
	if err != nil {
		klog.V(5).Error(err, "cannot close lock file")
		return false, err
	}
	return true, nil
}
