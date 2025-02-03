.PHONY: wasm build

wasm:
	GOOS=js GOARCH=wasm go build -o output/main.wasm ./cmd/main.go && \
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" ./output/ || \
	cp "$$(go env GOROOT)/misc/wasm/wasm_exec.js" ./output/ || \
	cp "/usr/local/go/lib/wasm/wasm_exec.js" ./output/ || \
	cp "/usr/local/go/misc/wasm/wasm_exec.js" ./output/ || \
	wget -O ./output/wasm_exec.js https://raw.githubusercontent.com/golang/go/refs/heads/release-branch.go1.24/lib/wasm/wasm_exec.js

build: wasm

webserver:
	# This runs a simple webserver via python serving the files in the output directory
	cd output && python3 -m http.server 8080
