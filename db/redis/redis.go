package redis

import (
	"encoding/json"
	"fmt"
	redis "gopkg.in/redis.v4"

	"github.com/micro/db-srv/db"
	mdb "github.com/micro/db-srv/proto/db"
	"github.com/micro/go-micro/registry"
	"hash/fnv"
	"sync"
	"time"
)

type redisDriver struct{}

type redisDB struct {
	sync.RWMutex
	redisIndex string
	redisType  string
	url        string
	clients    map[string]*redis.Client
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) & 16
}

var (
	RedisPass = ""
)

func init() {
	db.Drivers["redis"] = new(redisDriver)
}

func (d *redisDriver) NewDB(nodes ...*registry.Node) (db.DB, error) {

	if len(nodes) == 0 {
		return nil, db.ErrNotAvailable
	}
	url := fmt.Sprintf("%s:%d", nodes[0].Address, nodes[0].Port)

	return &redisDB{
		url:     url,
		clients: make(map[string]*redis.Client),
	}, nil
}

func (d *redisDB) Init(mdb *mdb.Database) error {
	d.Lock()
	defer d.Unlock()
	d.clients[mdb.Table] = redis.NewClient(&redis.Options{
		Addr:     d.url,
		Password: RedisPass,
		DB:       hash(mdb.Table),
	})
	d.redisIndex = mdb.Name
	d.redisType = mdb.Table

	return nil
}

func (d *redisDB) Close() error {
	d.Lock()
	defer d.Unlock()
	client, ok := d.clients[d.redisType]
	if !ok {
		client.Close()
	}

	return nil
}

// Reads a generic record from a database
func (d *redisDB) Read(id string) (*mdb.Record, error) {
	d.RLock()
	defer d.RUnlock()

	client, ok := d.clients[d.redisType]
	if !ok {
		return nil, db.ErrNotFound
	}

	r := client.Get(id)

	pl := &mdb.Record{}
	val := r.Val()
	if len(val) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(val), &pl); err != nil {
		return nil, err
	}

	return pl, nil
}

// Creates a generic record in a database
func (d *redisDB) Create(r *mdb.Record) error {
	d.RLock()
	defer d.RUnlock()
	if r.Created == 0 {
		r.Created = time.Now().Unix()
	}
	r.Updated = time.Now().Unix()
	res, err := json.Marshal(r)

	if err != nil {
		return err
	}
	client, ok := d.clients[d.redisType]
	if ok {
		client.Set(r.Id, string(res), 0)
	}

	return nil
}

// U of CRUD for generic records
func (d *redisDB) Update(r *mdb.Record) error {
	d.RLock()
	defer d.RUnlock()
	if r.Created == 0 {
		r.Created = time.Now().Unix()
	}
	r.Updated = time.Now().Unix()
	res, err := json.Marshal(r)
	if err != nil {
		return err
	}
	client, ok := d.clients[d.redisType]
	if ok {
		client.Set(r.Id, string(res), 0)
	}

	return nil
}

// D of CRUD for generic records
func (d *redisDB) Delete(id string) error {
	d.RLock()
	defer d.RUnlock()
	client, ok := d.clients[d.redisType]
	if ok {
		client.Del(id)
	}

	return nil
}

// Search records
func (d *redisDB) Search(md map[string]string, from, to, limit, offset int64, reverse bool) ([]*mdb.Record, error) {
	d.RLock()
	defer d.RUnlock()

	var records []*mdb.Record

	// TODO is search applicable to redis?

	return records, nil
}
