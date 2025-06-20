#!/bin/bash
echo "📊 Current Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo -e "\n🔍 Ports in use:"
ss -tuln | grep -E ':(8080|8081|9092|9001)'