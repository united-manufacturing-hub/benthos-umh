# Sparkplug B Plugin - Test Infrastructure
# Build targets for clean separation of test concerns via build tags
# Following the test strategy documented in README.md

.PHONY: help test test-unit test-payload test-flow test-all test-manual test-integration generate-vectors clean coverage

# Default: fast unit tests only (<3s)
test: test-unit

# Help target - show available commands
help:
	@echo "Sparkplug B Test Targets:"
	@echo "  test          - Fast unit tests only (<3s)"
	@echo "  test-unit     - Unit tests (AliasCache, TopicParser, etc.)"
	@echo "  test-payload  - Unit + payload tests (<5s)"
	@echo "  test-flow     - Unit + payload + flow tests (<8s)"  
	@echo "  test-all      - All offline tests (<8s)"
	@echo "  test-manual   - Instructions for integration tests"
	@echo "  test-integration - Real MQTT broker tests (requires manual setup)"
	@echo "  generate-vectors - Regenerate test vectors"
	@echo "  clean         - Clean temporary files"
	@echo "  coverage      - Generate coverage report"

# Unit tests (no build tag = always runs, default)
test-unit:
	@echo "🧪 Running Sparkplug unit tests (offline, <3s)..."
	cd .. && go test -race -v ./sparkplug_plugin -run "Unit"

# Payload decoding tests (+2s)
test-payload:
	@echo "📦 Running unit + payload tests (<5s)..."
	cd .. && go test -race -v -tags=payload ./sparkplug_plugin

# Flow/lifecycle tests (+3s)  
test-flow:
	@echo "🔄 Running unit + payload + flow tests (<8s)..."
	cd .. && go test -race -v -tags=flow ./sparkplug_plugin

# Full offline test suite (<8s total)
test-all:
	@echo "🚀 Running all offline tests (<8s)..."
	@$(MAKE) test-unit
	@$(MAKE) test-payload  
	@$(MAKE) test-flow
	@echo "✅ All offline tests completed successfully"

# Manual integration (requires developer to start mosquitto)
test-manual:
	@echo "🚀 Manual Integration Test Setup:"
	@echo "  1. Start mosquitto: docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2"
	@echo "  2. Run integration: make test-integration"
	@echo "  3. Cleanup: docker stop mosquitto && docker rm mosquitto"

# Integration tests (requires running MQTT broker)
test-integration:
	@echo "🔌 Running integration tests (requires MQTT broker)..."
	cd .. && go test -race -v -tags=integration ./sparkplug_plugin

# Regenerate test vectors (when spec changes)
generate-vectors:
	@echo "⚙️  Generating test vectors..."
	cd .. && go run ./sparkplug_plugin/cmd/gen-vectors > sparkplug_plugin/test_vectors.go
	cd .. && go fmt sparkplug_plugin/test_vectors.go
	@echo "✅ Test vectors regenerated"

# Clean up temporary files
clean:
	@echo "🧹 Cleaning temporary files..."
	rm -f cover.out coverage.html
	rm -rf tmp/
	@echo "✅ Clean complete"

# Show test coverage
coverage:
	@echo "📊 Generating coverage report..."
	cd .. && go test -race -v -tags=payload,flow -coverprofile=cover.out ./sparkplug_plugin
	cd .. && go tool cover -html=cover.out -o sparkplug_plugin/coverage.html
	@echo "📊 Coverage report: sparkplug_plugin/coverage.html"

# Validate infrastructure works
validate:
	@echo "🔍 Validating test infrastructure..."
	@echo "1. Checking if unit tests exist..."
	@test -f unit_test.go || echo "❌ unit_test.go missing"
	@echo "2. Checking if payload tests exist..."
	@test -f payload_test.go || echo "❌ payload_test.go missing"
	@echo "3. Checking if flow tests exist..."
	@test -f flow_test.go || echo "❌ flow_test.go missing"
	@echo "4. Checking if integration tests exist..."
	@test -f integration_test.go || echo "❌ integration_test.go missing"
	@echo "5. Checking if vector generator exists..."
	@test -f cmd/gen-vectors/main.go || echo "❌ cmd/gen-vectors/main.go missing"
	@echo "✅ Infrastructure validation complete" 