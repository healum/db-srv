package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/micro/go-micro/registry"
	"time"

	elib "github.com/mattbaird/elastigo/lib"
	"sync"

	"github.com/micro/db-srv/db"
	mdb "github.com/micro/db-srv/proto/db"
)

type elasticsearchDriver struct{}

type elasticsearchDB struct {
	sync.RWMutex
	elasticIndex string
	elasticType  string
	conn         *elib.Conn
}

func init() {
	db.Drivers["elasticsearch"] = new(elasticsearchDriver)
}

func (d *elasticsearchDriver) NewDB(nodes ...*registry.Node) (db.DB, error) {

	if len(nodes) == 0 {
		return nil, db.ErrNotAvailable
	}

	url := fmt.Sprintf("%s:%d", nodes[0].Address, nodes[0].Port)
	connection := elib.NewConn()
	connection.SetHosts([]string{url})

	// test the connection
	_, err := connection.Health()
	if err != nil {
		return nil, err
	}

	return &elasticsearchDB{
		conn: connection,
	}, nil
}

func (d *elasticsearchDB) Init(mdb *mdb.Database) error {
	d.Lock()
	defer d.Unlock()
	res, _ := d.conn.IndicesExists(mdb.Name)
	if !res {
		_, err := d.conn.CreateIndex(mdb.Name)
		if err != nil {
			return err
		}
	}
	d.elasticIndex = mdb.Name
	d.elasticType = mdb.Table

	return nil
}

func (d *elasticsearchDB) Close() error {
	d.Lock()
	defer d.Unlock()
	d.conn.Close()
	return nil
}

// Reads a generic record from a database
func (d *elasticsearchDB) Read(id string) (*mdb.Record, error) {
	d.RLock()
	defer d.RUnlock()
	r, err := d.conn.Get(d.elasticIndex, d.elasticType, id, nil)
	if err != nil {
		return nil, err
	}

	pl := &mdb.Record{}

	if err := json.Unmarshal(*r.Source, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}

// Creates a generic record in a database
func (d *elasticsearchDB) Create(r *mdb.Record) error {
	d.RLock()
	defer d.RUnlock()
	if r.Created == 0 {
		r.Created = time.Now().Unix()
	}
	r.Updated = time.Now().Unix()
	_, err := d.conn.Index(d.elasticIndex, d.elasticType, r.Id, nil, r)
	return err
}

// U of CRUD for generic records
func (d *elasticsearchDB) Update(r *mdb.Record) error {
	d.RLock()
	defer d.RUnlock()
	if r.Created == 0 {
		r.Created = time.Now().Unix()
	}
	r.Updated = time.Now().Unix()
	_, err := d.conn.Index(d.elasticIndex, d.elasticType, r.Id, nil, r)
	return err
}

// D of CRUD for generic records
func (d *elasticsearchDB) Delete(id string) error {
	d.RLock()
	defer d.RUnlock()
	_, err := d.conn.Delete(d.elasticIndex, d.elasticType, id, nil)
	return err
}

// Search terms are passed in the map
// Search returns all records if no search terms provided
func (d *elasticsearchDB) Search(md map[string]string, limit, offset int64, reverse bool) ([]*mdb.Record, error) {
	d.RLock()
	defer d.RUnlock()

	if limit <= 0 {
		limit = 10
	}

	if offset < 0 {
		offset = 0
	}

	size := fmt.Sprintf("%d", limit)
	offs := fmt.Sprintf("%d", offset)

	query := "*"

	if len(md) > 0 {
		query = ""
		// create statement for each key-val pair
		first := true
		for k, v := range md {
			if len(query) == 0 {
				query += " "
			} else {
				if first {
					first = false
				}else{
					query += " AND "
				}

				query += fmt.Sprintf(`%s:(%s)`, k, v)
			}
		}
	}
	sortDsl := &elib.SortDsl{Name: "created", IsDesc: reverse}
	elasticQuery := elib.Search(d.elasticIndex).Sort(sortDsl).Type(d.elasticType).Size(size).From(offs).Search(query)
	out, err := elasticQuery.Result(d.conn)
	if err != nil {
		return nil, err
	}
	var records []*mdb.Record
	for _, hit := range out.Hits.Hits {
		var rec *mdb.Record
		if err := json.Unmarshal(*hit.Source, &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}
