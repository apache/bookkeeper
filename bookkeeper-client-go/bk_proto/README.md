### Install protoc tool

```
go get github.com/gogo/protobuf/protoc-gen-gogofast
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/gogoproto
```

### Generate code:

```
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofast_out=. BookkeeperProtocol.proto
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofast_out=. DataFormats.proto
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofast_out=. DbLedgerStorageDataFormats.proto
```