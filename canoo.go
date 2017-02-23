package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/siddontang/go-mysql/canal"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user, must have replication privilege")
var password = flag.String("password", "", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var dataDir = flag.String("data-dir", "./tmp", "Path to store data, like master.info")

var serverID = flag.Int("server-id", 101, "Unique Server ID")
var mysqldump = flag.String("mysqldump", "mysqldump", "mysqldump execution path")

var dbs = flag.String("dbs", "test", "dump databases, seperated by comma")
var tables = flag.String("tables", "", "dump tables, seperated by comma, will overwrite dbs")
var tableDB = flag.String("table_db", "test", "database for dump tables")
var ignoreTables = flag.String("ignore_tables", "", "ignore tables, must be database.table format, separated by comma")
var httpAddr = flag.String("http", ":8009", "HTTP serve address")

var (
	bktMeta   = []byte("meta")
	bktItem   = []byte("item")
	bktBiblio = []byte("biblio")
)

type Main struct {
	db    *bolt.DB
	canal *canal.Canal
}

func main() {
	flag.Parse()

	m := new(Main)
	m.canal = m.newCanal()

	// tables to be ignored
	if len(*ignoreTables) == 0 {
		subs := strings.Split(*ignoreTables, ",")
		for _, sub := range subs {
			if seps := strings.Split(sub, "."); len(seps) == 2 {
				m.canal.AddDumpIgnoreTables(seps[0], seps[1])
			}
		}
	}

	// tables to be dumped
	if len(*tables) > 0 && len(*tableDB) > 0 {
		subs := strings.Split(*tables, ",")
		m.canal.AddDumpTables(*tableDB, subs...)
	} else if len(*dbs) > 0 {
		subs := strings.Split(*dbs, ",")
		m.canal.AddDumpDatabases(subs...)
	}

	// DB setup
	db, err := bolt.Open("bolt.db", 0640, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	m.db = db

	// Setup required buckets
	if err := m.setupBuckets(); err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	m.canal.RegRowsEventHandler(&rowsEventHandler{m})

	log.Println("Starting canal...")
	go m.runCanal()

	// TODO: update biblios
	go m.updateBiblios()

	log.Printf("Starting HTTP server listening at %v", *httpAddr)
	http.ListenAndServe(*httpAddr, newServer(m.db))

}

func (m Main) newCanal() *canal.Canal {
	// canal setup
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = *user
	cfg.Password = *password
	cfg.Flavor = *flavor
	cfg.DataDir = *dataDir

	cfg.ServerID = uint32(*serverID)
	cfg.Dump.ExecutionPath = *mysqldump
	cfg.Dump.DiscardErr = false

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Printf("create canal err %v", err)
		os.Exit(1)
	}
	return c
}

func (m Main) runCanal() {

	err := m.canal.Start()
	if err != nil {
		log.Printf("start canal err %v", err)
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	<-sc

	m.canal.Close()
}

// setup ensures DB is set up with required buckets.
func (m Main) setupBuckets() error {
	err := m.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktMeta, bktItem, bktBiblio} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// iterate items and aggregate into biblio bucket
func (m Main) updateBiblios() error {
	start := time.Now()
	err := m.db.Update(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bktItem).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			it := &item{}
			if err := json.Unmarshal(v, &it); err != nil {
				return err
			}
			var b *biblio
			// Biblio alreay exist?
			oldBib := tx.Bucket(bktBiblio).Get(i64tob(it.Biblionumber))
			if oldBib != nil {
				err := json.Unmarshal(oldBib, &b)
				if err != nil {
					log.Printf("Error decoding bib: %s", err)
					return err
				}
			} else {
				b = new(biblio)
			}
			b.updateBib(it)

			data, err := json.Marshal(b)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bktBiblio).Put(i64tob(b.Biblionumber), data); err != nil {
				log.Printf("Error updating biblio %d: %s", it.Biblionumber, err)
				return err
			}
			continue
		}
		return nil
	})
	elapsed := time.Since(start)
	log.Printf("Time updating biblios: %s", elapsed)
	return err
}

// append to existing biblio or make new
func (b *biblio) updateBib(it *item) {
	b.Biblionumber = it.Biblionumber
	b.Items++
	b.Issues += it.Issues
	b.Renewals += it.Renewals
	b.Reserves += it.Reserves
	if it.Available == true {
		if _, ok := b.Availability[it.Homebranch]; ok {
			b.Availability[it.Homebranch]++
		} else {
			b.Availability = make(map[string]int64)
			b.Availability[it.Homebranch]++
		}
	}
}

// Eventhandler is a
type rowsEventHandler struct {
	m *Main
}

// Main struct
type biblio struct {
	Biblionumber                      int64 // required
	Items, Issues, Renewals, Reserves int64
	Title                             string
	Availability                      map[string]int64
}

// item is the items table in koha
type item struct {
	Itemnumber, Biblionumber, Biblioitemnumber int64 // required
	Homebranch                                 string
	Barcode                                    string
	Issues, Renewals, Reserves                 int64
	Available                                  bool
}

func (r *rowsEventHandler) String() string {
	return "RowsEventHandler"
}

// handler for Rows events
func (r *rowsEventHandler) Do(e *canal.RowsEvent) error {
	switch e.Table.Name {
	case "items":
		i := r.newItem(e.Rows)
		switch e.Action {
		case "insert":
			if err := r.insertItem(i); err != nil {
				return err
			}
		case "update":
			// TODO: UPDATE items
		}
	case "reserves":
		// TODO: UPDATE found == T|W -> unavailable, DELETE cancellationdate -> available
		//fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
	case "issues":
		// TODO: INSERT -> unavailable
		//fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
	case "old_issues":
		// TODO: INSERT returndate -> available
		//fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
	}

	return nil
}

// creates an item struct from item row event
func (r *rowsEventHandler) newItem(rs [][]interface{}) *item {
	i := &item{}
	for _, v := range rs {
		i.Itemnumber = v[0].(int64)
		i.Biblionumber = v[1].(int64)
		i.Biblioitemnumber = v[2].(int64)
		if _, ok := v[6].(string); ok {
			i.Homebranch = v[6].(string)
		}
		if _, ok := v[3].(string); ok {
			i.Barcode = v[3].(string)
		}
		if _, ok := v[21].(int64); ok {
			i.Issues = v[21].(int64)
		}
		if _, ok := v[22].(int64); ok {
			i.Issues = v[22].(int64)
		}
		if _, ok := v[23].(int64); ok {
			i.Reserves = v[23].(int64)
		}
		i.Available = r.itemAvailable(v)
	}
	return i
}

// inserts a new item
func (r *rowsEventHandler) insertItem(i *item) error {
	err := r.m.db.Update(func(tx *bolt.Tx) error {
		data, err := encodeItem(i)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bktItem).Put(i64tob(i.Itemnumber), data); err != nil {
			fmt.Printf("Error inserting item %d: %s", i.Itemnumber, err)
			return err
		}
		return nil
	})

	return err
}

// item availability based on row values
func (r *rowsEventHandler) itemAvailable(v []interface{}) bool {

	if v[13].(int64) != 0 { // notforloan
		return false
	}
	if v[14].(int64) != 0 { // damaged
		return false
	}
	if v[15].(int64) != 0 { // itemlost
		return false
	}
	if v[17].(int64) != 0 { // withdrawn
		return false
	}
	if _, ok := v[32].(string); ok == true { // onloan a string?
		return false
	}
	return true
}

// Utility functions
func encodeItem(i *item) ([]byte, error) {
	return json.Marshal(i)
}

func encodeStats(s *stats) ([]byte, error) {
	return json.Marshal(s)
}

func decodeItem(b []byte) (item, error) {
	var i item
	err := json.Unmarshal(b, &i)
	return i, err
}

func i64tob(i int64) []byte {
	b := make([]byte, 8)
	binary.PutVarint(b, i)
	return b
}

func itob(i int) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, i)
	return buf.Bytes()
}

func btoi64(b []byte) (int64, int) {
	return binary.Varint(b)
}

// u32tob converts a uint32 into a 4-byte slice.
func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// btou32 converts a 4-byte slice into an uint32.
func btou32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
