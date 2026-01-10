#!/bin/bash

# echo "⏳ Waiting for NameNode to be ready..."

# echo "⏳ Waiting for NameNode RPC (8020) and HTTP (9870) ports..."
# while ! nc -z namenode 8020 || ! nc -z namenode 9870; do
#   echo "   Ports not ready yet. Retrying in 5s..."
#   sleep 5
# done

# echo "✅ Ports open. Waiting for HDFS to exit safe mode..."
# while hdfs dfsadmin -safemode get 2>/dev/null | grep -q "ON"; do
#   echo "   Still in Safe Mode. Retrying in 5s..."
#   sleep 5
# done

# echo "✅ NameNode & DataNode ready."

# echo "📁 Creating HDFS directories..."
# hdfs dfs -mkdir -p /data/raw/traffic
# hdfs dfs -chmod -R 777 /data
# hdfs dfs -chown -R root:supergroup /data  # if your script runs as 'root'

# echo "🔍 Final HDFS structure:"
# hdfs dfs -ls /data/raw

# echo "✅ HDFS initialization completed"

set -e

echo "⏳ Waiting for HDFS to respond to 'hdfs dfs -ls /'..."
while ! hdfs dfs -ls / > /dev/null 2>&1; do
  echo "   HDFS not ready. Retrying in 5s..."
  sleep 5
done

echo "✅ HDFS is ready. Creating directories..."
hdfs dfs -mkdir -p /data/raw/traffic

# Skip chmod/chown if dfs_permissions_enabled=false (recommended for dev)
echo "✅ Directories created."

echo "🔍 HDFS root contents:"
hdfs dfs -ls /