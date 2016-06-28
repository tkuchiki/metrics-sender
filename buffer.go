package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"time"
)

type Buffer struct {
	db *bolt.DB
}

func NewBuffer(dbpath, mode string) (Buffer, error) {
	b := Buffer{}
	m, err := strconv.ParseInt(mode, 8, 0)
	if err != nil {
		return b, err
	}

	db, err := bolt.Open(dbpath, os.FileMode(m), &bolt.Options{Timeout: 5 * time.Second})

	b.db = db
	return b, err
}

func (b *Buffer) Write(bucketName string, metrics []Metric) error {
	var err error
	var bucket *bolt.Bucket
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		var value []byte
		value, err = json.Marshal(metrics)
		if err != nil {
			return err
		}

		key := fmt.Sprint(time.Now().UnixNano())
		err = bucket.Put([]byte(key), value)

		return err
	})
}

func (b *Buffer) Read(bucketName string, num int) (map[string][]Metric, error) {
	var err error
	var values map[string][]Metric
	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return errors.New("Bucket not found")
		}

		values = make(map[string][]Metric)
		c := bucket.Cursor()
		i := 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			metrics := make([]Metric, 0)

			if err = json.Unmarshal(v, &metrics); err != nil {
				return err
			}

			values[string(k)] = metrics
			i++
			if num != 0 && i >= num {
				break
			}
		}

		return nil
	})

	if err == nil {

	}

	return values, err
}

func (b *Buffer) Delete(bucketName, key string) error {
	var err error
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return err
		}

		err = bucket.Delete([]byte(key))

		return err
	})
}

func (b *Buffer) Close() {
	if b.db != nil {
		b.db.Close()
	}
}
