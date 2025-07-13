.PHONY: imports

.SILENT: imports

imports:
	go run golang.org/x/tools/cmd/goimports@latest -l -w -local traderkit-server .
