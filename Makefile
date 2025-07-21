.PHONY: imports protoc

.SILENT: imports

imports:
	go run golang.org/x/tools/cmd/goimports@latest -l -w -local traderkit-server .

# Usage: `make protoc file=path/to/your/file.proto`
protoc:
	./protobuf.sh $(file)