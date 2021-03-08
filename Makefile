all: clean benthos benthos-lambda

clean:
	@rm -f tmp/bin/benthos tmp/bin/benthos-lambda tmp/benthos-lambda.zip

test:
	go test ./...

benthos:
	go build -o tmp/bin/benthos cmd/benthos/main.go

benthos-lambda:
	go build -o tmp/bin/benthos-lambda cmd/benthos-lambda/main.go
	zip -m -j tmp/benthos-lambda.zip ./tmp/bin/benthos-lambda

format:
	@gofmt -s -w .

.PHONY: test
