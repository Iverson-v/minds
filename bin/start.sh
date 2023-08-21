#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
META_SERVER_PATH="$SCRIPT_DIR/../metaServer/metaServer-1.0.jar"
DATA_SERVER_PATH="$SCRIPT_DIR/../dataServer/dataServer-1.0.jar"

# 使用传入的参数，或默认为 localhost:2181
#ZOOKEEPER_ADDR=${1:-localhost:2181}
ZOOKEEPER_ADDR=10.0.0.201:2181

# 启动metaServer的两个实例
echo "启动 metaServer ..."
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $META_SERVER_PATH --spring.profiles.active=default &
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $META_SERVER_PATH --spring.profiles.active=ms1 &

# 启动dataServer的四个实例
echo "启动 dataServer ..."
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $DATA_SERVER_PATH --spring.profiles.active=default &
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $DATA_SERVER_PATH --spring.profiles.active=ds1 &
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $DATA_SERVER_PATH --spring.profiles.active=ds2 &
java -Dzookeeper.addr=$ZOOKEEPER_ADDR -jar $DATA_SERVER_PATH --spring.profiles.active=ds3 &

echo "所有服务已启动"
