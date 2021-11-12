package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	cf, key := req.GetCf(), req.GetKey()
	reader, _ := server.storage.Reader(req.GetContext())
	val, err := reader.GetCF(cf, key)
	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}
	if val == nil {
		resp.NotFound = true
	}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key, val := req.GetKey(), req.GetValue()
	cf := req.GetCf()
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    cf,
				Key:   key,
				Value: val,
			},
		},
	}
	resp := &kvrpcpb.RawPutResponse{}
	if err := server.storage.Write(nil, batch); err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key, cf := req.GetKey(), req.GetCf()
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  cf,
				Key: key,
			},
		},
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	if err := server.storage.Write(nil, batch); err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey, limit, cf := req.GetStartKey(), req.GetLimit(), req.GetCf()
	reader, _ := server.storage.Reader(nil)
	iterator := reader.IterCF(cf)
	iterator.Seek(startKey)

	var kvs []*kvrpcpb.KvPair
	var err error
	for i := 0; uint32(i) < limit; i++ {
		if !iterator.Valid() {
			break
		}
		item := iterator.Item()
		key := item.Key()
		var val []byte
		val, err = item.Value()
		if err != nil {
			break
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: val})
		iterator.Next()
	}
	resp := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}
