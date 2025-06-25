#!/bin/bash

# Copyright 2025 UMH Systems GmbH
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

set -e

echo "🚀 Starting Sparkplug B Integration Test with Local Mosquitto"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to cleanup
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    docker stop mosquitto >/dev/null 2>&1 || true
    docker rm mosquitto >/dev/null 2>&1 || true
    
    # Kill background processes
    if [ ! -z "$EDGE_PID" ]; then
        kill $EDGE_PID >/dev/null 2>&1 || true
    fi
    if [ ! -z "$HOST_PID" ]; then
        kill $HOST_PID >/dev/null 2>&1 || true
    fi
    
    echo -e "${GREEN}✅ Cleanup completed${NC}"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT INT TERM

echo -e "${BLUE}📦 Step 1: Starting Mosquitto MQTT Broker...${NC}"
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:1.6
sleep 3

echo -e "${BLUE}🔍 Step 2: Verifying Mosquitto is running...${NC}"
if ! docker ps | grep -q mosquitto; then
    echo -e "${RED}❌ Mosquitto failed to start${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Mosquitto is running on port 1883${NC}"

echo -e "${BLUE}🏭 Step 3: Starting Edge Node (background)...${NC}"
cd /workspaces/benthos-umh
./benthos -c config/sparkplug-device-level-test.yaml > /tmp/edge-node.log 2>&1 &
EDGE_PID=$!
echo -e "${GREEN}✅ Edge Node started (PID: $EDGE_PID)${NC}"

echo -e "${BLUE}⏱️  Step 4: Waiting for Edge Node to publish BIRTH message...${NC}"
sleep 5

echo -e "${BLUE}🖥️  Step 5: Starting Primary Host (10 seconds)...${NC}"
timeout 10 ./benthos -c config/sparkplug-device-level-primary-host.yaml || true

echo -e "\n${YELLOW}📊 Integration Test Results:${NC}"
echo -e "${BLUE}Edge Node Log (last 20 lines):${NC}"
tail -20 /tmp/edge-node.log | grep -E "(NBIRTH|NDATA|Published|Error)" || echo "No specific messages found"

echo -e "\n${GREEN}🎉 Integration test completed!${NC}"
echo -e "${BLUE}💡 Manual verification:${NC}"
echo -e "   • Check that Edge Node published BIRTH and DATA messages"
echo -e "   • Check that Primary Host received and processed messages"
echo -e "   • Both should show Sparkplug B message flow" 