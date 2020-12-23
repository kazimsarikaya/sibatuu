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
	. "github.com/kazimsarikaya/sibatuu/internal"
	"github.com/kazimsarikaya/sibatuu/internal/backupfs"
	klog "k8s.io/klog/v2"
	"time"
)

type LockManager struct {
	fs           backupfs.BackupFS
	lockerId     uuid.UUID
	abortFlag    bool
	refresher    *time.Ticker
	endRefresher chan bool
	rh           *RepositoryHelper
}

func NewLockManager(fs backupfs.BackupFS, rh *RepositoryHelper) (*LockManager, error) {
	lockerId, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	lm := &LockManager{fs: fs, lockerId: lockerId, abortFlag: false, rh: rh}
	lm.endRefresher = make(chan bool)
	return lm, nil
}

func (lm *LockManager) abort() {
	lm.abortFlag = true
	lm.endRefresher <- true
}

func (lm *LockManager) acquireLock() (bool, error) {
	lockIdStr := lm.lockerId.String()

	err := lm.fs.Mkdirs(LocksControlDir)
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
			klog.V(5).Error(err, "cannot generate locker timestamp data")
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
			klog.V(5).Infof("lock detected, may be dead lock")
			r, err := lm.fs.Open(LockFile)
			if err == nil {
				lockTimeData := make([]byte, 128)
				rc, err := r.Read(lockTimeData)
				if err == nil {
					var lockTime time.Time
					if lockTime.UnmarshalBinary(lockTimeData[:rc]) == nil {
						if time.Now().Sub(lockTime) > LockTimeout {
							klog.V(5).Infof("lock is dead, make zero len")
							r.Close()
							lm.releaseLock()
						}
					}
				}
				r.Close()
			}
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

	err = lm.refreshLock()
	if err != nil {
		lm.fs.Delete(lockControlFile)
		return false, nil
	}
	lm.fs.Delete(lockControlFile)
	lm.refresher = time.NewTicker(LockRefreshInterval)
	go func() {
		var errcnt int = 0
		for {
			select {
			case <-lm.endRefresher:
				return
			case <-lm.refresher.C:
				err := lm.refreshLock()
				if err != nil {
					klog.V(5).Error(err, "cannot refresh my lock")
					errcnt += 1
					if errcnt == 3 {
						lm.rh.AbortBackup()
					}
				} else {
					errcnt = 0
				}
			}
		}
	}()
	return true, nil
}

func (lm *LockManager) refreshLock() error {
	lockData, err := time.Now().MarshalBinary()
	if err != nil {
		klog.V(5).Error(err, "cannot generate lock timestamp data")
		return err
	}

	w, err := lm.fs.Create(LockFile)
	if err != nil {
		klog.V(5).Error(err, "cannot create lock file")
		return err
	}

	_, err = w.Write(lockData)
	if err != nil {
		klog.V(5).Error(err, "cannot write lock id data to lock file ")
		return err
	}
	err = w.Close()
	if err != nil {
		klog.V(5).Error(err, "cannot write lock id data")
		return err
	}
	klog.V(5).Infof("i gathered lock")
	return nil
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
	lm.endRefresher <- true
	return true, nil
}
