package subnet

import (
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/flynn/flannel/Godeps/_workspace/src/github.com/coreos/go-etcd/etcd"
	log "github.com/flynn/flannel/Godeps/_workspace/src/github.com/golang/glog"
)

var (
	ErrSubnetExists          = errors.New("subnet exists")
	ErrEtcdEventIndexCleared = errors.New("event index cleared")
)

type Response struct {
	Subnets    map[string][]byte
	Index      uint64
	Expiration *time.Time
	Action     string
}

type subnetRegistry interface {
	GetConfig() ([]byte, error)
	GetSubnets() (*Response, error)
	CreateSubnet(sn, data string, ttl uint64) (*Response, error)
	UpdateSubnet(sn, data string, ttl uint64) (*Response, error)
	WatchSubnets(since uint64, stop chan bool) (*Response, error)
}

type EtcdConfig struct {
	Endpoints []string
	Keyfile   string
	Certfile  string
	CAFile    string
	Prefix    string
}

type etcdSubnetRegistry struct {
	mux     sync.Mutex
	cli     *etcd.Client
	etcdCfg *EtcdConfig
}

func newEtcdClient(c *EtcdConfig) (*etcd.Client, error) {
	if c.Keyfile != "" || c.Certfile != "" || c.CAFile != "" {
		return etcd.NewTLSClient(c.Endpoints, c.Certfile, c.Keyfile, c.CAFile)
	} else {
		return etcd.NewClient(c.Endpoints), nil
	}
}

func newEtcdSubnetRegistry(config *EtcdConfig) (subnetRegistry, error) {
	r := &etcdSubnetRegistry{
		etcdCfg: config,
	}

	var err error
	r.cli, err = newEtcdClient(config)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (esr *etcdSubnetRegistry) GetConfig() ([]byte, error) {
	key := path.Join(esr.etcdCfg.Prefix, "config")
	resp, err := esr.client().Get(key, false, false)
	if err != nil {
		return nil, err
	}
	return []byte(resp.Node.Value), nil
}

func (esr *etcdSubnetRegistry) GetSubnets() (*Response, error) {
	res := &Response{Subnets: make(map[string][]byte)}

	key := path.Join(esr.etcdCfg.Prefix, "subnets")
	r, err := esr.client().Get(key, false, true)
	if err != nil {
		if e, ok := err.(*etcd.EtcdError); ok && e.ErrorCode == etcdKeyNotFound {
			// key not found: treat it as empty set
			res.Index = e.Index
			return res, nil
		}
		return nil, err
	}
	for _, node := range r.Node.Nodes {
		res.Subnets[node.Key] = []byte(node.Value)
	}
	res.Index = r.EtcdIndex
	return res, nil
}

func (esr *etcdSubnetRegistry) CreateSubnet(sn, data string, ttl uint64) (*Response, error) {
	key := path.Join(esr.etcdCfg.Prefix, "subnets", sn)
	resp, err := esr.client().Create(key, data, ttl)
	if err != nil {
		// if etcd returned Key Already Exists, try again.
		if e, ok := err.(*etcd.EtcdError); ok && e.ErrorCode == etcdKeyAlreadyExists {
			return nil, ErrSubnetExists
		}
		return nil, err
	}
	ensureExpiration(resp, ttl)
	return &Response{Index: resp.EtcdIndex, Expiration: resp.Node.Expiration}, nil
}

func (esr *etcdSubnetRegistry) UpdateSubnet(sn, data string, ttl uint64) (*Response, error) {
	key := path.Join(esr.etcdCfg.Prefix, "subnets", sn)
	resp, err := esr.client().Set(key, data, ttl)
	if err != nil {
		return nil, err
	}
	ensureExpiration(resp, ttl)
	return &Response{Index: resp.EtcdIndex, Expiration: resp.Node.Expiration}, nil
}

func (esr *etcdSubnetRegistry) WatchSubnets(since uint64, stop chan bool) (res *Response, err error) {
	res = &Response{Subnets: make(map[string][]byte)}
	defer func() {
		if e, ok := err.(*etcd.EtcdError); ok && e.ErrorCode == etcdEventIndexCleared {
			err = ErrEtcdEventIndexCleared
		}
	}()

	for {
		key := path.Join(esr.etcdCfg.Prefix, "subnets")
		raw, err := esr.client().RawWatch(key, since, true, nil, stop)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				return nil, nil
			} else {
				return nil, err
			}
		}

		if len(raw.Body) == 0 {
			// etcd timed out, go back but recreate the client as the underlying
			// http transport gets hosed (http://code.google.com/p/go/issues/detail?id=8648)
			esr.resetClient()
			continue
		}

		r, err := raw.Unmarshal()
		if err != nil {
			return nil, err
		}
		for _, node := range r.Node.Nodes {
			res.Subnets[node.Key] = []byte(node.Value)
		}
		res.Index = r.Node.ModifiedIndex
		res.Action = r.Action
		return res, nil
	}
}

func (esr *etcdSubnetRegistry) client() *etcd.Client {
	esr.mux.Lock()
	defer esr.mux.Unlock()
	return esr.cli
}

func (esr *etcdSubnetRegistry) resetClient() {
	esr.mux.Lock()
	defer esr.mux.Unlock()

	var err error
	esr.cli, err = newEtcdClient(esr.etcdCfg)
	if err != nil {
		panic(fmt.Errorf("resetClient: error recreating etcd client: %v", err))
	}
}

func ensureExpiration(resp *etcd.Response, ttl uint64) {
	if resp.Node.Expiration == nil {
		// should not be but calc it ourselves in this case
		log.Info("Expiration field missing on etcd response, calculating locally")
		exp := time.Now().Add(time.Duration(ttl) * time.Second)
		resp.Node.Expiration = &exp
	}
}
