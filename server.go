package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"net/http"
	"strconv"
	"strings"
)

var errNotFound = errors.New("not found")

type server struct {
	db *bolt.DB
}

func newServer(db *bolt.DB) server {
	return server{db: db}
}

func (s server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path, "/")
	if len(path) < 1 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch path[1] {
	case "item":
		i, err := strconv.Atoi(path[2])
		if err != nil {
			http.Error(w, "itemnumber must be an integer", http.StatusBadRequest)
			return
		}
		if err := s.getItem(i, w, r); err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
	case "list":
		s.list(bktItem)
	default:
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

}

func (s server) list(bucket []byte) {
	s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucket)).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
}

func (s server) getItem(itemnumber int, w http.ResponseWriter, r *http.Request) error {

	var recJson []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktItem).Get(i64tob(int64(itemnumber)))
		if b == nil {
			return errNotFound
		}
		recJson = make([]byte, len(b))
		copy(recJson, b)
		return nil
	}); err != nil {
		return err
	}
	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "gzip")

	gz := gzip.NewWriter(w)
	defer gz.Close()
	if _, err := gz.Write(recJson); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	return nil
}
