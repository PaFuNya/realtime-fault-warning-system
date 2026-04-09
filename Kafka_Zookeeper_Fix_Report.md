# Kafka & Zookeeper 集群故障修复报告

## 故障现象
在 `master` (192.168.45.100)、`slave1` (192.168.45.101)、`slave2` (192.168.45.102) 上，Kafka 和 Zookeeper 无法正常启动并创建节点。

## 故障原因详细分析（为什么会有“两个 Zookeeper”）

### 1. 什么是“两个 Zookeeper”？
在你们的服务器环境中，确实存在两套 Zookeeper 运行机制的冲突，这也是导致 Kafka 无法正常工作的罪魁祸首：
*   **第一套：Kafka 自带的 Zookeeper（伪集群/单机模式）**
    *   **来源**：Kafka 安装包内自带了一个简易版的 Zookeeper（通过 `zookeeper-server-start.sh` 和 `config/zookeeper.properties` 启动）。
    *   **表现**：在后台进程中显示为 `QuorumPeerMain config/zookeeper.properties`。
    *   **问题**：它通常只作为单机测试使用，没有配置集群间的通信（没有配置 2888 和 3888 选举端口）。如果三台机器都运行了这个，它们就是三个完全独立、互不认识的单机 Zookeeper。
*   **第二套：独立安装的 Zookeeper 集群（真正的集群模式）**
    *   **来源**：专门安装在 `/opt/module/zookeeper` 目录下的 Zookeeper 软件。
    *   **表现**：在后台进程中显示为 `QuorumPeerMain /opt/module/zookeeper/bin/../conf/zoo.cfg` 或类似路径。
    *   **问题**：它配置了 `server.1`, `server.2`, `server.3`，是真正的集群。但由于默认端口也是 `2181`，当它尝试启动时，发现端口已经被上面那个“假 Zookeeper”占用了。

### 2. 冲突导致的“脑裂”与脏数据
*   由于 Kafka 自带的 Zookeeper 抢占了 `2181` 端口，真正的 Zookeeper 集群无法正常启动或无法正常通信。
*   随后启动的 Kafka 进程连接到了本机的 `2181` 端口。因为三台机器上的 `2181` 端口背后都是互不认识的单机版 Zookeeper，这就导致：
    *   `master` 的 Kafka 认为自己是集群的唯一老大。
    *   `slave1` 的 Kafka 也认为自己是集群的唯一老大。
    *   `slave2` 同理。
*   这就是典型的**脑裂（Split-Brain）**现象。此时如果你去创建 Topic，要么报错，要么只在某一台机器上生效，无法在三台机器间同步副本，导致集群彻底瘫痪，并在磁盘上留下了错乱的元数据（脏数据）。

### 3. 为什么之前启动不了？
*   **端口被死锁**：冲突的进程在后台没有被彻底杀死（使用普通的 `kill` 或者脚本没有生效），导致重启时永远报错“端口被占用”。
*   **脏数据干扰**：即使杀死了进程，之前脑裂期间在 `/opt/module/zookeeper/zkData/version-2` 和 `/opt/module/kafka/data/` 目录中写入的错误集群 ID 和元数据依然存在。Kafka 启动时读取到这些相互矛盾的旧数据，就会直接报错退出。

### 4. 自动创建的 1 分区 Topic 是怎么回事？
*   在脑裂期间或刚刚恢复时，后台可能有一些数据采集程序（如 Flume、Flink 等）正在不断尝试往 Kafka 发送数据。
*   Kafka 有一个默认特性：如果向一个不存在的 Topic 发送数据，它会自动创建这个 Topic，但默认只给 1 个分区。这就是为什么你要求的 6 个 Topic 中，有 4 个被偷偷建成了 1 个分区。

---

## 详细解决步骤

### 1. 彻底清理残留和冲突进程
在三台节点上，使用严格匹配的进程清理命令，强制杀死所有相关 Java 进程，释放 `2181` 和 `9092` 端口：
```bash
ps -ef | grep '[K]afka' | awk '{print $2}' | xargs -r kill -9
ps -ef | grep '[Q]uorumPeerMain' | awk '{print $2}' | xargs -r kill -9
ps -ef | grep '[Z]ooKeeperServerMain' | awk '{print $2}' | xargs -r kill -9
```

### 2. 清理历史脏数据 (重置集群状态)
为防止旧的单机模式元数据影响新的集群启动，在所有节点清空 ZK 和 Kafka 的数据目录，但**保留 Zookeeper 的 `myid` 文件**：
```bash
# 清理 Zookeeper 数据与日志
rm -rf /opt/module/zookeeper/zkData/version-2
rm -rf /opt/module/zookeeper/logs/*

# 清理 Kafka 数据与日志
rm -rf /opt/module/kafka/data/*
rm -rf /opt/module/kafka/logs/*
```

### 3. 按正确顺序启动 Zookeeper 集群
在三台节点上统一使用独立的 Zookeeper 启动脚本启动集群：
```bash
source /etc/profile
/opt/module/zookeeper/bin/zkServer.sh start
```
启动后通过 `/opt/module/zookeeper/bin/zkServer.sh status` 验证，确认一台为 `leader`，另外两台为 `follower`。

### 4. 启动 Kafka 集群
在 Zookeeper 集群状态正常后，依次在三台节点上启动 Kafka：
```bash
source /etc/profile
/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties
```

### 5. 创建并修改指定 Topic 为 3 个分区
在 Kafka 成功启动后，创建所需的 6 个 Topic。对于部分被后台任务抢先自动创建的 1 分区 Topic，使用 `--alter` 命令强制将其扩容至 3 个分区。
```bash
# 创建 Topic
/opt/module/kafka/bin/kafka-topics.sh --create --bootstrap-server master:9092 --replication-factor 1 --partitions 3 --topic device_state
/opt/module/kafka/bin/kafka-topics.sh --create --bootstrap-server master:9092 --replication-factor 1 --partitions 3 --topic sensor_metrics

# 扩容被自动创建的 Topic
/opt/module/kafka/bin/kafka-topics.sh --alter --bootstrap-server master:9092 --topic highfreq_sensor --partitions 3
/opt/module/kafka/bin/kafka-topics.sh --alter --bootstrap-server master:9092 --topic ChangeRecord --partitions 3
/opt/module/kafka/bin/kafka-topics.sh --alter --bootstrap-server master:9092 --topic sensor_raw --partitions 3
/opt/module/kafka/bin/kafka-topics.sh --alter --bootstrap-server master:9092 --topic log_raw --partitions 3
```

经过以上步骤，Zookeeper 及 Kafka 集群已成功恢复，所有要求的节点（Topic）均已成功创建并拥有 3 个分区。
