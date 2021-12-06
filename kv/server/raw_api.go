package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var response kvrpcpb.RawGetResponse
	// Get the reader and handle error if it fails
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		response.Error = err.Error()
		return &response, fmt.Errorf("Fail to get reader")
	}
	defer reader.Close()
	// Get the value from the reader
	val, _ := reader.GetCF(req.Cf, req.Key)
	response.Value = val
	if len(val) == 0 {
		response.NotFound = true
	}
	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var response kvrpcpb.RawPutResponse
	if req == nil {
		return &response, nil
	}
	// Init modify from the put request
	var modify storage.Modify
	modify.Data = storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	// Write modify into the database, and handle error if possible
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		response.Error = err.Error()
		return &response, fmt.Errorf("Fail to write into database")
	}
	return &response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var response kvrpcpb.RawDeleteResponse
	if req == nil {
		return &response, nil
	}
	// Init modify from the delete request
	var modify storage.Modify
	modify.Data = storage.Delete{
		Key:   req.GetKey(),
		Cf:    req.GetCf(),
	}
	// Write modify into the database, and handle error if possible
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		response.Error = err.Error()
		return &response, fmt.Errorf("Fail to write into database")
	}
	return &response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var response kvrpcpb.RawScanResponse
	// Get the reader and handle error if it fails
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.RegionError = util.RaftstoreErrToPbError(err)
		response.Error = err.Error()
		return &response, fmt.Errorf("Fail to get reader")
	}
	defer reader.Close()
	// Get the iterator from the reader
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	// Use iterator to scan
	iter.Seek(req.GetStartKey())
	limit := req.GetLimit()
	for iter.Valid() && limit > 0 {
		var kv = new(kvrpcpb.KvPair)
		item := iter.Item()
		kv.Key = item.Key()
		kv.Value, err = item.Value()
		if err != nil {
			response.RegionError = util.RaftstoreErrToPbError(err)
			response.Error = err.Error()
			return &response, fmt.Errorf("Fail to get a valid value")
		}
		response.Kvs = append(response.Kvs, kv)
		limit--;
		iter.Next()
	}
	return &response, nil
}
