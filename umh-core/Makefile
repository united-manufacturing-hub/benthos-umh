.PHONY: build run clean test

IMAGE_NAME = umh-core
TAG = latest

build:
	docker build -t $(IMAGE_NAME):$(TAG) -f Dockerfile .
	
clean:
	docker stop $(IMAGE_NAME) || true
	docker rm $(IMAGE_NAME) || true
	docker rmi $(IMAGE_NAME):$(TAG) || true

test:
	go test -v ./...

test-empty: clean build
	@mkdir -p $(PWD)/data
	@cp $(PWD)/examples/example-config-empty.yaml $(PWD)/data/config.yaml
	docker run \
		--name $(IMAGE_NAME) \
		-v $(PWD)/data:/data \
		-p 8081:8080 \
		$(IMAGE_NAME):$(TAG)

test-hello: clean build
	@mkdir -p $(PWD)/data
	@cp $(PWD)/examples/example-config.yaml $(PWD)/data/config.yaml
	docker run \
		--name $(IMAGE_NAME) \
		-v $(PWD)/data:/data \
		-p 8081:8080 \
		-p 4195:4195 \
		$(IMAGE_NAME):$(TAG)