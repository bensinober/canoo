package main

import (
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

var (
	bktMeta   = []byte("meta")
	bktItem   = []byte("item")
	bktRecord = []byte("record")
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
	if err := m.setup(); err != nil {
		log.Fatal(err)
	}

	// Register a handler to handle RowsEvent
	m.canal.RegRowsEventHandler(&rowsEventHandler{m})

	log.Println("Starting canal...")
	go m.run()

	log.Printf("Starting HTTP server listening at %v", ":8080")
	http.ListenAndServe(":8080", newServer(m.db))

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
		fmt.Printf("create canal err %v", err)
		os.Exit(1)
	}
	return c
}

func (m Main) run() {

	err := m.canal.Start()
	if err != nil {
		fmt.Printf("start canal err %v", err)
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	<-sc

	m.canal.Close()
}

// setup ensures DB is set up with required buckets.
func (m Main) setup() error {
	err := m.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktMeta, bktItem} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Eventhandler is a
type rowsEventHandler struct {
	m *Main
}

// Main struct
type record struct {
	Biblionumber                      int64 // required
	Items, Issues, Renewals, Reserves uint32
	Title                             string
	Availability                      []string
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
		fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
	case "issues":
		// TODO: INSERT -> unavailable
		fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
	case "old_issues":
		// TODO: INSERT returndate -> available
		fmt.Printf("%s %s: %#v\n", e.Action, e.Table.Name, e.Rows)
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
		json, err := encode(i)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bktItem).Put(i64tob(i.Itemnumber), json); err != nil {
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
func encode(i *item) ([]byte, error) {
	return json.Marshal(i)
}

func decode(b []byte) (item, error) {
	var i item
	err := json.Unmarshal(b, &i)
	return i, err
}

func i64tob(i int64) []byte {
	b := make([]byte, 8)
	binary.PutVarint(b, i)
	return b
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