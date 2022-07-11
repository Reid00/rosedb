package rosedb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/flower-corp/rosedb/logger"
	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	t.Run("default", func(t *testing.T) {
		opts := DefaultOptions(path)
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})

	t.Run("mmap", func(t *testing.T) {
		opts := DefaultOptions(path)
		opts.IoType = MMap
		db, err := Open(opts)
		defer destroyDB(db)
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})
}

func TestLogFileGC(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.LogFileGCInterval = time.Second * 7
	opts.LogFileGCRatio = 0.00001
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue16B())
		assert.Nil(t, err)
	}

	var deleted [][]byte
	rand.Seed(time.Now().Unix())
	for i := 0; i < 100000; i++ {
		k := rand.Intn(writeCount)
		key := GetKey(k)
		err := db.Delete(key)
		assert.Nil(t, err)
		deleted = append(deleted, key)
	}

	time.Sleep(time.Second * 12)
	for _, key := range deleted {
		_, err := db.Get(key)
		assert.Equal(t, err, ErrKeyNotFound)
	}
}

func TestRoseDB_Backup(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	defer destroyDB(db)
	if err != nil {
		t.Error("open db err ", err)
	}

	for i := 0; i < 10; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	backupPath := filepath.Join("/tmp", "rosedb-backup")
	err = db.Backup(backupPath)
	assert.Nil(t, err)

	// open the backup database
	opts2 := DefaultOptions(backupPath)
	db2, err := Open(opts2)
	assert.Nil(t, err)
	defer destroyDB(db2)
	val, err := db2.Get(GetKey(4))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRoseDB_Expire(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testRoseDBExpire(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testRoseDBExpire(t, MMap, KeyValueMemMode)
	})
}

func testRoseDBExpire(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	t.Run("normal", func(t *testing.T) {
		for kType := String; kType <= ZSet; kType++ {
			err = db.Expire(GetKey(31), time.Second*2, kType)
			assert.Equal(t, ErrKeyNotFound, err)
		}
		// String set key
		err = db.Set(GetKey(55), GetValue16B())
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*1, String)
		assert.Nil(t, err)
		// List
		err = db.RPush(GetKey(55), GetValue16B(), GetValue16B())
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*1, List)
		assert.Nil(t, err)
		// Hash
		err = db.HSet(GetKey(55), GetKey(55), GetValue16B())
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*1, Hash)
		assert.Nil(t, err)
		// Set
		err = db.SAdd(GetKey(55), GetKey(55), GetKey(555))
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*1, Set)
		assert.Nil(t, err)
		// Zset
		err = db.ZAdd(GetKey(111), 1.2, GetKey(55))
		err = db.ZAdd(GetKey(111), 2.3, GetKey(555))
		assert.Nil(t, err)
		err = db.Expire(GetKey(111), time.Second*1, ZSet)
		assert.Nil(t, err)

		time.Sleep(2 * time.Second)
		// Check Key
		// String
		_, err = db.Get(GetKey(55))
		assert.Equal(t, ErrExpiredKey, err)
		// List
		val, err := db.RPop(GetKey(55))
		assert.Nil(t, val)
		assert.Equal(t, ErrExpiredKey, err)
		// Hash
		val, err = db.HGet(GetKey(55), GetKey(55))
		assert.Nil(t, val)
		assert.Equal(t, ErrExpiredKey, err)
		// Set
		ok := db.SIsMember(GetKey(55), GetKey(55))
		assert.Equal(t, false, ok)
		// ZSet
		_, err = db.ZRange(GetKey(111), 0, -1)
		assert.Equal(t, ErrExpiredKey, err)

	})

	t.Run("set-twice", func(t *testing.T) {
		err := db.Set(GetKey(66), GetValue16B())
		assert.Nil(t, err)

		// String set
		err = db.Set(GetKey(55), GetValue16B())
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*100, String)
		err = db.Expire(GetKey(55), time.Second*1, String)
		assert.Nil(t, err)
		// List
		err = db.RPush(GetKey(55), GetValue16B())
		assert.Equal(t, ErrExpiredKey, err)
		err = db.Expire(GetKey(55), time.Second*100, List)
		err = db.Expire(GetKey(55), time.Second*1, List)
		assert.Equal(t, ErrExpiredKey, err)
		// Hash
		err = db.HSet(GetKey(55), GetKey(55), GetValue16B())
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*100, Hash)
		err = db.Expire(GetKey(55), time.Second*1, Hash)
		assert.Nil(t, err)
		// Set
		err = db.SAdd(GetKey(55), GetKey(55))
		assert.Nil(t, err)
		err = db.Expire(GetKey(55), time.Second*100, Set)
		err = db.Expire(GetKey(55), time.Second*1, Set)
		assert.Nil(t, err)
		// Zset
		err = db.ZAdd(GetKey(111), 1, GetKey(55))
		assert.Nil(t, err)
		err = db.Expire(GetKey(111), time.Second*100, ZSet)
		err = db.Expire(GetKey(111), time.Second*1, ZSet)
		assert.Nil(t, err)

		time.Sleep(1 * time.Second)
		// Check Key
		// String
		_, err = db.Get(GetKey(55))
		assert.Equal(t, ErrExpiredKey, err)
		// List
		val, err := db.RPop(GetKey(55))
		assert.Nil(t, val)
		assert.Equal(t, ErrExpiredKey, err)
		// Hash
		val, err = db.HGet(GetKey(55), GetKey(55))
		assert.Equal(t, ErrExpiredKey, err)
		assert.Nil(t, val)
		// Set
		ok := db.SIsMember(GetKey(55), GetKey(55))
		assert.Equal(t, false, ok)
		// ZSet
		_, err = db.ZRange(GetKey(111), 0, 2)
		assert.Equal(t, ErrExpiredKey, err)
	})
}

func TestRoseDB_TTL(t *testing.T) {
	path := filepath.Join("/tmp", "rosedb")
	opts := DefaultOptions(path)
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	for kType := String; kType <= ZSet; kType++ {
		t1, err := db.TTL(GetKey(111), kType)
		assert.Equal(t, int64(0), t1)
		assert.Equal(t, ErrKeyNotFound, err)
	}

	// String set key
	err = db.Set(GetKey(55), GetValue16B())
	assert.Nil(t, err)
	err = db.Expire(GetKey(55), time.Second*10, String)
	assert.Nil(t, err)
	// List
	err = db.RPush(GetKey(55), GetValue16B())
	assert.Nil(t, err)
	err = db.Expire(GetKey(55), time.Second*10, List)
	assert.Nil(t, err)
	// Hash
	err = db.HSet(GetKey(55), GetKey(55), GetValue16B())
	assert.Nil(t, err)
	err = db.Expire(GetKey(55), time.Second*10, Hash)
	assert.Nil(t, err)
	// Set
	err = db.SAdd(GetKey(55), GetKey(55))
	assert.Nil(t, err)
	err = db.Expire(GetKey(55), time.Second*10, Set)
	assert.Nil(t, err)
	// Zset
	err = db.ZAdd(GetKey(111), 1, GetKey(55))
	assert.Nil(t, err)
	err = db.Expire(GetKey(111), time.Second*100, ZSet)
	err = db.Expire(GetKey(111), time.Second*10, ZSet)
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)

	for kType := String; kType <= ZSet; kType++ {
		if kType == ZSet {
			t1, err := db.TTL(GetKey(111), kType)
			assert.Equal(t, int64(8), t1)
			assert.Nil(t, err)
		} else {
			t1, err := db.TTL(GetKey(55), kType)
			assert.Equal(t, int64(8), t1)
			assert.Nil(t, err)
		}
	}
}

func destroyDB(db *RoseDB) {
	if db != nil {
		_ = db.Close()
		if runtime.GOOS == "windows" {
			time.Sleep(time.Millisecond * 100)
		}
		err := os.RemoveAll(db.opts.DBPath)
		if err != nil {
			logger.Errorf("destroy db err: %v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue16B() []byte {
	return GetValue(16)
}

func GetValue128B() []byte {
	return GetValue(128)
}

func GetValue4K() []byte {
	return GetValue(4096)
}

func GetValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}
