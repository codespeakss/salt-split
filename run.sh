#!/bin/bash
# run.sh - 构建并运行 Flink 程序

set -e

echo "清理 target 目录... "
rm -rf target

echo "🚀 构建中..."
mvn clean package -DskipTests

echo "✅ 构建完成，正在运行..."
java -jar target/salt-split-1.0-SNAPSHOT.jar

