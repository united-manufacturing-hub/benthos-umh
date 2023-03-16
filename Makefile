all: clean target

clean:
	@rm -rf target tmp/bin tmp/benthos-*.zip

target:
	@mkdir -p tmp/bin
	@goreleaser build --single-target --snapshot --id benthos \
		--output ./tmp/bin/benthos
test:
	@go test ./...

lint:
	@golangci-lint run

format:
	@golangci-lint run --fix

.PHONY: clean target test lint format
