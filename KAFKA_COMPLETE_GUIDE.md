# 📚 Kafka文件处理完整指南

## 📋 概述

本项目提供基于Kafka的分布式文件处理解决方案，专门用于处理存储在华为云OBS中的文件。系统包含三个核心组件，形成完整的处理链路：

```
任务生成器 → Kafka队列 → Worker处理器 → 结果通知
    ↓           ↓          ↓           ↓
  扫描OBS    处理任务    文件处理    状态反馈
```

### 🎯 核心组件

1. **🔄 任务生成器** (`task_generator`) - 扫描OBS文件并生成处理任务
2. **⚙️ 单进程Worker** (`worker`) - 适合轻量级文件处理
3. **🚀 多进程Worker** (`pool_worker`) - 适合大文件和高并发处理

### 🌟 系统特点

- **分布式处理** - 支持多Worker实例并行处理
- **容错性强** - 自动重试和错误恢复机制
- **资源优化** - 智能文件清理和内存管理
- **实时监控** - 完整的处理状态追踪
- **高可扩展** - 水平扩展支持更高吞吐量

## 🚀 快速开始

### 1. 环境准备

#### 安装依赖
```bash
# 使用pixi（推荐）
pixi install

# 或使用pip
pip install -e .
```

#### 配置环境
复制并编辑环境配置：
```bash
cp env.example .env
# 编辑 .env 文件，填入实际配置
```

### 2. 基础配置

#### 最小配置示例（.env文件）
```bash
# ===== Kafka配置 =====
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TASKS=file-processing-tasks
KAFKA_RESULT_TOPIC=file-processing-results
KAFKA_GROUP_ID=obs-workers

# ===== OBS配置 =====
OBS_ACCESS_KEY=your_access_key
OBS_SECRET_KEY=your_secret_key
OBS_SERVER=https://obs.cn-north-4.myhuaweicloud.com
OBS_REGION=cn-north-4
OBS_SOURCE_BUCKET=input-files
OBS_DEST_BUCKET=processed-files

# ===== Worker配置 =====
POOL_SIZE=4
CLEANUP_FILES=true
CLEANUP_ON_ERROR=false
```

### 3. 一键启动

```bash
# 终端1: 生成处理任务
SOURCE_BUCKET=my-bucket FILE_PREFIX=data/ pixi run generate-tasks

# 终端2: 启动Worker处理
pixi run worker

# 终端3: 监控处理结果（可选）
kafka-console-consumer --bootstrap-server localhost:9092 --topic file-processing-results
```

## 📤 任务生成器详解

### 功能说明
任务生成器负责扫描OBS存储桶，发现需要处理的文件，并生成相应的Kafka处理任务。

### 使用方式

#### 1. 批量任务生成
```bash
# 扫描整个桶
SOURCE_BUCKET=my-bucket pixi run generate-tasks

# 按前缀过滤（推荐）
SOURCE_BUCKET=my-bucket FILE_PREFIX=data/2024/08/ pixi run generate-tasks

# 限制文件数量
SOURCE_BUCKET=my-bucket MAX_FILES=500 pixi run generate-tasks

# 指定目标桶
SOURCE_BUCKET=input-bucket DEST_BUCKET=output-bucket pixi run generate-tasks
```

#### 2. 单个文件任务
```bash
# 处理特定文件
SINGLE_FILE=urgent/data.csv SOURCE_BUCKET=my-bucket pixi run generate-single-task

# 指定输出路径
SINGLE_FILE=data.txt DEST_BUCKET=results DEST_KEY=processed/data.json pixi run generate-single-task
```

#### 3. 预配置任务
```bash
# 使用预设的批量任务配置
pixi run generate-batch-tasks
```

### 生成的任务消息格式

```json
{
  "key": "data/input.csv",                    // 必需：文件路径
  "bucket": "source-bucket",                  // 可选：源桶名
  "dest_bucket": "processed-bucket",          // 可选：目标桶名
  "dest_key": "processed/input.csv.json",    // 可选：目标路径
  "size": 1024,                              // 文件大小
  "last_modified": "2025-08-25T12:00:00Z",   // 修改时间
  "generated_at": "2025-08-25T12:05:00Z"     // 生成时间
}
```

### 高级功能

#### 编程式任务生成
```python
from chroma_ollama.task_generator import TaskGenerator

generator = TaskGenerator()

# 从文件列表生成任务
files = ["data/file1.csv", "data/file2.json"]
tasks = generator.generate_task_from_file_list(files, bucket="my-bucket")

# 发送到Kafka
generator.send_tasks_to_kafka(tasks, batch_size=50)

generator.close()
```

## ⚙️ Worker处理器详解

### 处理流程

每个Worker的标准处理流程：

```
1. 消费Kafka任务消息
   ↓
2. 从OBS下载源文件到本地
   ↓
3. 执行文件解析/处理逻辑
   ↓
4. 上传处理结果到目标OBS桶
   ↓
5. 发送处理结果通知到Kafka
   ↓
6. 清理本地临时文件
   ↓
7. 提交Kafka消息偏移量
```

### 单进程Worker

#### 适用场景
- 文件较小（< 10MB）
- 处理逻辑简单
- 资源受限环境
- 调试和开发阶段

#### 启动方式
```bash
# 基础启动
pixi run worker

# 自定义配置启动
CLEANUP_FILES=false KAFKA_GROUP_ID=debug-group pixi run worker
```

#### 特点
- **轻量级** - 内存占用小
- **简单稳定** - 单线程处理，易于调试
- **适合小文件** - 处理速度适中

### 多进程Worker

#### 适用场景
- 大文件处理（> 10MB）
- 复杂的数据转换
- 高吞吐量需求
- 生产环境

#### 启动方式
```bash
# 基础启动（4个进程）
pixi run pool-worker

# 自定义进程数
POOL_SIZE=8 pixi run pool-worker

# 高性能配置
POOL_SIZE=16 CLEANUP_FILES=true pixi run pool-worker
```

#### 特点
- **高性能** - 多进程并行处理
- **可扩展** - 支持动态调整进程数
- **适合大文件** - 有效利用多核CPU

### Worker配置优化

#### 性能调优
```bash
# 高吞吐量配置
POOL_SIZE=16                      # 增加进程数
KAFKA_MAX_POLL_RECORDS=1000       # 批量消费消息
CLEANUP_FILES=true                # 及时清理文件
KAFKA_SESSION_TIMEOUT_MS=30000    # 调整会话超时
```

#### 调试配置
```bash
# 调试模式
CLEANUP_FILES=false               # 保留文件便于检查
CLEANUP_ON_ERROR=false           # 失败时不清理
KAFKA_AUTO_OFFSET_RESET=earliest # 重新处理所有消息
```

#### 容错配置
```bash
# 高可靠性配置
KAFKA_ENABLE_AUTO_COMMIT=false   # 手动提交偏移量
CLEANUP_ON_ERROR=true            # 失败时清理文件
KAFKA_MAX_POLL_INTERVAL_MS=600000 # 增加处理超时
```

## 📊 结果通知系统

### 通知机制
每个文件处理完成后，Worker会发送详细的结果通知到专门的Kafka Topic。

### 成功通知格式
```json
{
  "timestamp": "2025-08-25T12:30:00Z",
  "worker_type": "single_process",          // Worker类型
  "status": "success",                      // 处理状态
  "source": {
    "bucket": "input-bucket",
    "key": "data/input.csv"
  },
  "destination": {
    "bucket": "output-bucket",
    "key": "data/input.csv.parsed.json"
  },
  "file_info": {
    "input_size": 1024,                     // 输入文件大小
    "output_size": 2048,                    // 输出文件大小
    "input_size_formatted": "1.0 KB",      // 格式化大小
    "output_size_formatted": "2.0 KB"
  }
}
```

### 失败通知格式
```json
{
  "timestamp": "2025-08-25T12:30:00Z",
  "worker_type": "multi_process",
  "status": "failed",
  "source": {
    "bucket": "input-bucket",
    "key": "data/corrupted.csv"
  },
  "error": "文件格式不支持",               // 错误信息
  "file_info": {
    "input_size": 512,
    "output_size": 0
  }
}
```

### 监控结果
```bash
# 实时监控所有结果
kafka-console-consumer --bootstrap-server localhost:9092 --topic file-processing-results

# 过滤成功的处理
kafka-console-consumer --bootstrap-server localhost:9092 --topic file-processing-results | grep "success"

# 过滤失败的处理
kafka-console-consumer --bootstrap-server localhost:9092 --topic file-processing-results | grep "failed"
```

## 🔧 完整配置参考

### 环境变量完整列表

#### Kafka配置
```bash
# 基础连接
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TASKS=file-processing-tasks        # 任务队列Topic
KAFKA_RESULT_TOPIC=file-processing-results     # 结果通知Topic
KAFKA_GROUP_ID=obs-workers                     # 消费组ID

# 消费者配置
KAFKA_AUTO_OFFSET_RESET=latest                 # earliest|latest
KAFKA_ENABLE_AUTO_COMMIT=true                  # 自动提交偏移量
KAFKA_AUTO_COMMIT_INTERVAL_MS=5000            # 提交间隔
KAFKA_SESSION_TIMEOUT_MS=10000                # 会话超时
KAFKA_HEARTBEAT_INTERVAL_MS=3000              # 心跳间隔
KAFKA_MAX_POLL_INTERVAL_MS=300000             # 最大轮询间隔
KAFKA_MAX_POLL_RECORDS=500                    # 每次轮询最大记录数

# 生产者配置
KAFKA_ENABLE_PRODUCER=true                     # 启用结果通知

# 安全配置（可选）
KAFKA_SECURITY_PROTOCOL=PLAINTEXT             # PLAINTEXT|SASL_PLAINTEXT|SASL_SSL|SSL
KAFKA_SASL_MECHANISM=PLAIN                    # PLAIN|SCRAM-SHA-256|SCRAM-SHA-512
KAFKA_SASL_USERNAME=your_username
KAFKA_SASL_PASSWORD=your_password
```

#### OBS配置
```bash
# 连接信息
OBS_ACCESS_KEY=your_access_key
OBS_SECRET_KEY=your_secret_key
OBS_SERVER=https://obs.cn-north-4.myhuaweicloud.com
OBS_REGION=cn-north-4

# 存储桶
OBS_SOURCE_BUCKET=input-files                  # 源文件桶
OBS_DEST_BUCKET=processed-files               # 结果文件桶

# 本地路径
OBS_DOWNLOAD_DIR=./work/downloads              # 下载目录
OBS_UPLOAD_DIR=./work/uploads                  # 上传目录
```

#### Worker配置
```bash
# 进程控制
POOL_SIZE=4                                   # 多进程Worker的进程数

# 文件管理
CLEANUP_FILES=true                            # 处理成功后清理文件
CLEANUP_ON_ERROR=false                       # 处理失败后清理文件

# 自定义解析器（高级）
DBC_FACTORY=my_module:create_parser           # DBC解析器工厂
```

#### 任务生成器配置
```bash
# 扫描配置
SOURCE_BUCKET=input-files                     # 要扫描的桶
FILE_PREFIX=data/2024/08/                    # 文件前缀过滤
DEST_BUCKET=processed-files                  # 目标桶（可选）
MAX_FILES=1000                               # 最大文件数

# 单文件模式
SINGLE_FILE=specific/file.txt                # 单个文件路径
```

## 🛠️ 运维和监控

### 日志监控

#### 结构化日志格式
Worker输出JSON格式的结构化日志：

```json
{
  "timestamp": "2025-08-25T12:00:00Z",
  "level": "info",
  "event": "处理完成",
  "filename": "worker.py",
  "func_name": "_process_message", 
  "lineno": 185,
  "file_key": "data/input.csv",
  "processing_time": 2.5,
  "input_size": 1024,
  "output_size": 2048
}
```

#### 日志查看技巧
```bash
# 实时查看Worker日志
pixi run worker | jq '.'

# 过滤特定事件
pixi run worker | jq 'select(.event == "处理完成")'

# 统计处理时间
pixi run worker | jq 'select(.processing_time) | .processing_time'
```

### 性能监控

#### 关键指标
- **处理吞吐量** - 每分钟处理文件数
- **平均处理时间** - 单个文件处理耗时
- **成功率** - 处理成功的比例
- **错误分布** - 各类错误的频率
- **资源使用** - CPU、内存、磁盘使用情况

#### 监控脚本示例
```bash
#!/bin/bash
# 简单的性能监控脚本

# 统计最近1小时的处理结果
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic file-processing-results \
  --from-beginning | \
  jq -r 'select(.timestamp > "'$(date -d '1 hour ago' -Iseconds)'") | .status' | \
  sort | uniq -c
```

### 故障排除

#### 常见问题及解决方案

1. **任务生成失败**
   ```bash
   # 症状：扫描OBS失败
   # 检查：OBS连接和权限
   # 解决：验证ACCESS_KEY和SECRET_KEY，确认桶权限
   ```

2. **Worker无法消费消息**
   ```bash
   # 症状：Worker启动但不处理文件
   # 检查：Kafka连接和Topic是否存在
   # 解决：确认BOOTSTRAP_SERVERS，创建Topic
   kafka-topics --bootstrap-server localhost:9092 --create --topic file-processing-tasks
   ```

3. **文件处理失败率高**
   ```bash
   # 症状：大量处理失败通知
   # 检查：文件格式和处理逻辑
   # 解决：保留失败文件进行分析
   CLEANUP_ON_ERROR=false pixi run worker
   ```

4. **磁盘空间不足**
   ```bash
   # 症状：下载失败或系统报错
   # 检查：临时目录空间使用
   # 解决：启用自动清理
   CLEANUP_FILES=true CLEANUP_ON_ERROR=true pixi run worker
   ```

5. **处理性能低**
   ```bash
   # 症状：吞吐量不满足需求
   # 检查：进程数和系统资源
   # 解决：增加进程数或部署多个Worker实例
   POOL_SIZE=16 pixi run pool-worker
   ```

## 📈 扩展和优化

### 水平扩展

#### 部署多个Worker实例
```bash
# 机器1
KAFKA_GROUP_ID=workers-group-1 pixi run pool-worker

# 机器2  
KAFKA_GROUP_ID=workers-group-1 pixi run pool-worker

# 机器3
KAFKA_GROUP_ID=workers-group-1 pixi run pool-worker
```

#### 按文件类型分组处理
```bash
# CSV文件处理组
KAFKA_GROUP_ID=csv-processors KAFKA_TOPIC_TASKS=csv-processing-tasks pixi run worker

# JSON文件处理组
KAFKA_GROUP_ID=json-processors KAFKA_TOPIC_TASKS=json-processing-tasks pixi run worker

# 日志文件处理组
KAFKA_GROUP_ID=log-processors KAFKA_TOPIC_TASKS=log-processing-tasks pixi run worker
```

### 自定义处理逻辑

#### 扩展文件解析器
```python
# 创建自定义解析器 custom_parser.py
from pathlib import Path
import json

def parse_csv_file(input_path: Path, output_path: Path) -> None:
    """自定义CSV解析逻辑"""
    import pandas as pd
    
    # 读取CSV
    df = pd.read_csv(input_path)
    
    # 执行自定义处理
    result = {
        "total_rows": len(df),
        "columns": list(df.columns),
        "summary": df.describe().to_dict()
    }
    
    # 写入结果
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

# 在Worker中使用
# 修改 worker.py 的 _parse_file 方法调用 parse_csv_file
```

### 集成其他服务

#### 数据库记录处理状态
```python
# 在处理完成后记录到数据库
def record_processing_result(file_key: str, status: str, error: str = None):
    import sqlite3
    
    conn = sqlite3.connect('processing_log.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO processing_log (file_key, status, error, timestamp)
        VALUES (?, ?, ?, datetime('now'))
    ''', (file_key, status, error))
    
    conn.commit()
    conn.close()
```

#### 邮件通知集成
```python
# 处理失败时发送邮件通知
def send_failure_notification(file_key: str, error: str):
    import smtplib
    from email.mime.text import MIMEText
    
    msg = MIMEText(f"文件处理失败: {file_key}\n错误: {error}")
    msg['Subject'] = '文件处理失败通知'
    msg['From'] = 'system@company.com'
    msg['To'] = 'admin@company.com'
    
    # 发送邮件逻辑...
```

## 🎯 最佳实践

### 1. 任务规划
- **按时间分批处理** - 避免一次性生成过多任务
- **合理设置前缀** - 使用有意义的目录结构
- **监控队列长度** - 防止任务积压

### 2. 资源管理
- **合理配置进程数** - 根据CPU核心数和内存容量
- **启用文件清理** - 防止磁盘空间耗尽
- **监控系统资源** - CPU、内存、磁盘、网络使用情况

### 3. 错误处理
- **保留失败文件** - 便于问题诊断和数据恢复
- **建立重试机制** - 对临时失败进行自动重试
- **监控错误率** - 及时发现系统问题

### 4. 运维监控
- **结构化日志** - 便于日志分析和问题定位
- **性能指标收集** - 建立处理性能基线
- **告警机制** - 异常情况及时通知

### 5. 安全考虑
- **密钥管理** - 安全存储OBS和Kafka凭证
- **网络安全** - 使用SSL/TLS加密传输
- **访问控制** - 最小权限原则

这个完整的Kafka文件处理系统为大规模、分布式的文件处理提供了强大而灵活的解决方案！🚀
