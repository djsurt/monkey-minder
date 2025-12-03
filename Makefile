PROTOBUF_FILES = $(shell find ./ -name '*.proto')
PROTOBUF_FILES_COMPILED = $(patsubst %.proto,%.pb.go,$(PROTOBUF_FILES))
PROTOBUF_FILES_COMPILED += $(patsubst %.proto,%_grpc.pb.go,$(PROTOBUF_FILES))

.PHONY: clean protos protos-go protos-py

clean:
	-find ./ -name '*.pb.go' -delete
	-find ./ \( -name '*_pb2.py' -or -name '*_pb2_grpc.py' \) -delete

protos: protos-go protos-py

protos-go: $(PROTOBUF_FILES_COMPILED)

%.pb.go %_grpc.pb.go: %.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $^

protos-py:
	python-grpc-tools-protoc --proto_path=. --python_out=tester/src --grpc_python_out=tester/src --pyi_out=tester/src $(PROTOBUF_FILES)
