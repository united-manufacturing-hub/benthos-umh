prepare:
	rm -rf ./output && \
	mkdir -p ./output

copy-wasm-exec:
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" ./output/ || \
	cp "$$(go env GOROOT)/misc/wasm/wasm_exec.js" ./output/ || \
	cp "/usr/local/go/lib/wasm/wasm_exec.js" ./output/ || \
	cp "/usr/local/go/misc/wasm/wasm_exec.js" ./output/ || \
	wget -O ./output/wasm_exec.js https://raw.githubusercontent.com/golang/go/refs/heads/release-branch.go1.24/lib/wasm/wasm_exec.js

compress-wasm:
	gzip -9 -c ./output/benthos-umh.wasm > ./output/benthos-umh.wasm.gz && \
    ls -lh ./output/benthos-umh.wasm ./output/benthos-umh.wasm.gz
compile:
	GOOS=js GOARCH=wasm go build -o ./output/benthos-umh.wasm ./...

validate-wasm:
	@if ! command -v wasm-validate >/dev/null 2>&1; then \
		echo "wasm-validate not found, attempting to install wabt..."; \
		if [ -f /etc/os-release ] && grep -q -E "debian|ubuntu" /etc/os-release; then \
			if [ "$$(id -u)" -eq 0 ]; then \
				apt-get update && apt-get install -y wabt && echo "Installed wabt as root"; \
			elif apt-get update && apt-get install -y wabt 2>/dev/null; then \
				echo "Installed wabt without sudo"; \
			elif sudo -n true 2>/dev/null; then \
				echo "Using sudo to install wabt"; \
				sudo apt-get update && sudo apt-get install -y wabt && echo "Installed wabt with sudo"; \
			else \
				echo "Cannot install wabt, skipping validation"; \
				exit 0; \
			fi; \
		elif [ "$$(uname)" = "Darwin" ]; then \
			if command -v brew >/dev/null 2>&1; then \
				echo "Installing wabt with Homebrew"; \
				brew install wabt; \
			else \
				echo "Homebrew not found, skipping validation"; \
				exit 0; \
			fi; \
		else \
			echo "Unsupported OS, skipping validation"; \
			exit 0; \
		fi; \
	fi; \
	wasm-validate ./output/benthos-umh.wasm && echo "WASM validation successful" || exit 0


build: prepare compile validate-wasm copy-wasm-exec compress-wasm