.PHONY: build run clean test

IMAGE_NAME = umh-lite-v2
TAG = latest

build:
	docker build -t $(IMAGE_NAME):$(TAG) -f Dockerfile ..

run: build
	docker run \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME):$(TAG)

clean:
	docker stop $(IMAGE_NAME) || true
	docker rm $(IMAGE_NAME) || true
	docker rmi $(IMAGE_NAME):$(TAG) || true

test:
	go test -v ./...

logs:
	docker logs -f $(IMAGE_NAME)

shell:
	docker exec -it $(IMAGE_NAME) sh 