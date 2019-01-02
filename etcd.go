package goutils

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/thinkphoebe/golog"
)

type Etcd struct {
	Client  *clientv3.Client
	timeout time.Duration
}

type EtcdVisitor interface {
	Visit(key string, val []byte) bool
}

func newContexTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.TODO(), nil
	}
	return context.WithTimeout(context.TODO(), time.Duration(timeout)*time.Second)
}

func (this *Etcd) Init(endpoints []string, timeout time.Duration) error {
	var err error
	this.Client, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout * time.Second,
	})
	this.timeout = timeout
	if err != nil {
		log.Fatalf("[etcd][SLA] clientv3.New got err [%v]", err)
	}
	return err
}

func (this *Etcd) Exit() {
	this.Client.Close()
}

func (this *Etcd) OpPut(key, val string, ttl int64) (*clientv3.Op, error) {
	opts := []clientv3.OpOption{}
	if ttl > 0 {
		resp, err := this.Client.Grant(context.TODO(), ttl)
		if err != nil {
			log.Errorf("[etcd][SLA] OpPut - cli.Grant got err [%v]", err)
			return nil, err
		}
		opts = append(opts, clientv3.WithLease(resp.ID))
	}
	op := clientv3.OpPut(key, val, opts...)
	return &op, nil
}

func (this *Etcd) Put(key, val string, ttl int64) error {
	op, err := this.OpPut(key, val, ttl)
	if err != nil {
		return err
	}
	ctx, cancel := newContexTimeout(this.timeout)
	_, err = this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	log.Debugf("[etcd] Put - key [%s], val [%s], ttl [%d], err [%v]", key, val, ttl, err)
	return err
}

func (this *Etcd) OpGet(key string, prefix bool) *clientv3.Op {
	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	op := clientv3.OpGet(key, opts...)
	return &op
}

func (this *Etcd) Get(key string, prefix bool) ([][]byte, error) {
	op := this.OpGet(key, prefix)
	ctx, cancel := newContexTimeout(this.timeout)
	resp, err := this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		log.Errorf("[etcd][SLA] Get - key [%s], prefix [%t], err [%v]", key, prefix, err)
		return nil, err
	}

	log.Debugf("[etcd] Get - key [%s], prefix [%t], count [%d]", key, prefix, len(resp.Get().Kvs))
	vals := make([][]byte, len(resp.Get().Kvs))
	for i, kv := range resp.Get().Kvs {
		vals[i] = kv.Value
		log.Debugf("[etcd] Get - [%d] key [%s], val [%s]", i, string(kv.Key), string(kv.Value))
	}
	return vals, nil
}

func (this *Etcd) OpDel(key string, prefix bool) *clientv3.Op {
	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	op := clientv3.OpDelete(key, opts...)
	return &op
}

func (this *Etcd) Del(key string, prefix bool) (int64, error) {
	op := this.OpDel(key, prefix)
	ctx, cancel := newContexTimeout(this.timeout)
	resp, err := this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	log.Debugf("[etcd] Del - key [%s], count [%d], err [%v]", key, resp.Del().Deleted, err)
	return resp.Del().Deleted, err
}

func (this *Etcd) CmpKeyNotExist(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
}

func (this *Etcd) Txn(cmps []clientv3.Cmp, ifs []clientv3.Op, elses []clientv3.Op) (*clientv3.TxnResponse, error) {
	ctx, cancel := newContexTimeout(this.timeout)
	txn := this.Client.Txn(ctx).If(cmps...)
	txn = txn.Then(ifs...)
	if len(elses) > 0 {
		txn = txn.Else(elses...)
	}
	resp, err := txn.Commit()
	cancel()
	return resp, err
}

func (this *Etcd) Watch(key, action string, prefix bool, callback func(key string, val []byte)) {
	log.Infof("[etcd][SLA] Watch %s - key [%s], prefix [%t]", action, key, prefix)

	et := mvccpb.DELETE
	if action == "PUT" {
		et = mvccpb.PUT
	}

	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	rch := this.Client.Watch(context.TODO(), key, opts...)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			if ev.Type == et {
				log.Debugf("[etcd][SLA] Watch %s - key [%s], val [%s]",
					action, string(ev.Kv.Key), string(ev.Kv.Value))
				go callback(string(ev.Kv.Key), ev.Kv.Value)
			}
		}
	}
}

func (this *Etcd) Walk(prefix string, visitor EtcdVisitor, opts []clientv3.OpOption) error {
	all_opts := []clientv3.OpOption{}
	all_opts = append(all_opts, clientv3.WithPrefix())
	all_opts = append(all_opts, clientv3.WithLimit(500))
	all_opts = append(all_opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	if opts != nil {
		all_opts = append(all_opts, opts...)
	}

	keyStart := prefix
	for {
		ctx, cancel := newContexTimeout(this.timeout)
		out, err := this.Client.Get(ctx, keyStart, all_opts...)
		cancel()
		if err != nil {
			log.Errorf("[etcd][SLA] Walk - cli.Get [%s] got error [%v]", keyStart, err)
			return err
		}
		if len(out.Kvs) == 0 {
			break
		}

		for _, kv := range out.Kvs {
			log.
				Debugf("[etcd] Walk - prefix [%s] keyStart [%s] got [%s:%s]", prefix, keyStart, kv.Key, kv.Value)
			if !visitor.Visit(string(kv.Key), kv.Value) {
				log.Errorf("[etcd][SLA] Walk - canceled by visitor")
				return nil
			}
		}
		keyStart = string(out.Kvs[len(out.Kvs)-1].Key) + "\x00"
	}
	return nil
}
