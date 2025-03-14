.PHONY: build run clean test

IMAGE_NAME = umh-core
TAG = latest

build:
	docker build -t $(IMAGE_NAME):$(TAG) -f Dockerfile ..

run: build
	docker run \
		--name $(IMAGE_NAME) \
		-v $(PWD)/data:/data \
		$(IMAGE_NAME):$(TAG)

clean:
	docker stop $(IMAGE_NAME) || true
	docker rm $(IMAGE_NAME) || true
	docker rmi $(IMAGE_NAME):$(TAG) || true

test:
	go test -v ./...

test-hello: clean build
	@mkdir -p $(PWD)/data
	@cp $(PWD)/examples/example-config.yaml $(PWD)/data/config.yaml
	docker run \
		--name $(IMAGE_NAME) \
		-v $(PWD)/data:/data \
		-e LOGGING_LEVEL=debug \
		$(IMAGE_NAME):$(TAG)

test-hello-100: clean build
	@mkdir -p $(PWD)/data
	@cp $(PWD)/examples/example-config-100.yaml $(PWD)/data/config.yaml
	docker run \
		--name $(IMAGE_NAME) \
		-v $(PWD)/data:/data \
		-e LOGGING_LEVEL=production \
		$(IMAGE_NAME):$(TAG)

logs:
	docker logs -f $(IMAGE_NAME)

shell:
	docker exec -it $(IMAGE_NAME) sh 