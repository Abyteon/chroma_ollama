# Chroma + Ollama 向量库项目 (改进版)

一个基于ChromaDB和Ollama的本地向量搜索解决方案，支持JSON文档的向量化和语义搜索，以及Kafka驱动的文件处理。

## 🚀 新功能和改进

### 1. 现代化的项目结构
- 使用 `pyproject.toml` 替代 `requirements.txt`
- 支持 `pixi` 环境管理
- 更好的包结构和模块化设计

### 2. 类型安全的配置管理
- 使用 Pydantic 进行配置验证
- 支持环境变量和 `.env` 文件
- 自动类型检查和验证

### 3. 结构化日志系统
- 使用 structlog 提供结构化日志
- 支持 JSON 格式输出
- 包含时间戳和调用者信息

### 4. 强大的重试机制
- 使用 tenacity 库提供指数退避重试
- 针对不同服务（Kafka、OBS、Ollama）的专门重试策略
- 可配置的重试参数

### 5. 改进的错误处理
- 更好的异常处理和日志记录
- 优雅的错误恢复机制
- 详细的错误信息

### 6. 📨 Kafka结果通知系统
- 处理完成后自动发送结果通知到指定Topic
- 包含成功/失败状态、文件信息、处理时间等详细信息
- 支持下游系统实时获取处理结果

### 7. 🧹 智能文件清理系统
- 处理完成后自动清理本地临时文件
- 可配置的清理策略（成功时/失败时）
- 避免磁盘空间耗尽，提高系统稳定性

## 📦 安装

### 使用 pixi (推荐)

```bash
# 安装 pixi
curl -fsSL https://pixi.sh/install.sh | bash

# 进入项目目录
cd chroma_ollama

# 安装依赖
pixi install

# 激活环境
pixi shell
```

### 使用 pip

```bash
# 安装依赖
pip install -e .

# 或者安装开发依赖
pip install -e ".[dev]"
```

## 🔧 配置

### 环境变量配置

复制 `.env.example` 为 `.env` 并按需修改：

```env
# ChromaDB 配置
CHROMA_DB_DIR=./chroma_db
CHROMA_COLLECTION=docs
OLLAMA_EMBED_MODEL=nomic-embed-text
OLLAMA_BASE_URL=http://localhost:11434

# Kafka 配置 (可选)
KAFKA_BOOTSTRAP_SERVERS=your.kafka:9092
KAFKA_TOPIC_TASKS=obs-file-tasks
KAFKA_GROUP_ID=obs-worker-group
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# OBS 配置 (可选)
OBS_ACCESS_KEY=your_access_key
OBS_SECRET_KEY=your_secret_key
OBS_SERVER=https://obs.cn-north-4.myhuaweicloud.com
OBS_REGION=cn-north-4
OBS_SOURCE_BUCKET=your-source-bucket
OBS_DEST_BUCKET=your-dest-bucket
```

### 配置验证

```python
from chroma_ollama.config import config

# 配置会自动验证
print(f"ChromaDB 目录: {config.chroma.db_dir}")
print(f"嵌入模型: {config.chroma.embedding_model}")
```

## 📚 使用指南

### 1. 文档摄入

```bash
# 使用 pixi（默认配置）
pixi run ingest

# 使用环境变量配置
DATA_DIR=./data BATCH_SIZE=100 pixi run ingest

# 或直接运行
python -m chroma_ollama.ingest
```

### 2. 文档搜索

```bash
# 使用 pixi（需设置搜索查询）
SEARCH_QUERY="最新价格政策" SEARCH_K=5 pixi run search

# 或直接运行
SEARCH_QUERY="最新价格政策" python -m chroma_ollama.search
```

### 3. Kafka任务生成器

```bash
# 扫描OBS桶生成处理任务
SOURCE_BUCKET=my-bucket FILE_PREFIX=data/ pixi run generate-tasks

# 生成单个文件处理任务
SINGLE_FILE=important.csv SOURCE_BUCKET=my-bucket pixi run generate-single-task

# 批量生成任务（使用预配置）
pixi run generate-batch-tasks
```

### 4. Kafka Worker

```bash
# 简单 Worker
pixi run worker

# 多进程 Pooled Worker
pixi run pool-worker
```

## 🛠️ 开发

### 代码格式化

```bash
# 使用 black 格式化代码
black .

# 使用 isort 排序导入
isort .

# 使用 ruff 进行 linting
ruff check .
```

### 类型检查

```bash
# 使用 mypy 进行类型检查
mypy .
```

### 测试

```bash
# 运行测试
pytest

# 运行测试并生成覆盖率报告
pytest --cov=chroma_ollama

# 运行改进功能测试
python test_improvements.py
```

## 📊 性能优化

### 1. 批处理优化
- 支持批量文档摄入，减少API调用次数
- 可配置的批处理大小

### 2. 内存优化
- 使用生成器处理大文件
- 支持流式处理

### 3. 并发处理
- 多进程池支持
- 异步I/O操作

## 🔍 监控和日志

### 结构化日志

```python
from chroma_ollama.logging import get_logger

logger = get_logger(__name__)
logger.info("处理开始", file_count=10, batch_size=100)
```

### 日志配置

```python
from chroma_ollama.logging import configure_logging

# 配置日志
configure_logging(
    level="INFO",
    json_format=True,
    include_timestamp=True,
    include_caller=True,
)
```

## 🔄 重试机制

### 预定义重试装饰器

```python
from chroma_ollama.retry import retry_on_ollama_error, retry_on_kafka_error

@retry_on_ollama_error
def call_ollama_api():
    # 自动重试 Ollama API 调用
    pass

@retry_on_kafka_error
def kafka_operation():
    # 自动重试 Kafka 操作
    pass
```

### 自定义重试策略

```python
from chroma_ollama.retry import create_retry_decorator

custom_retry = create_retry_decorator(
    max_attempts=5,
    max_delay=30.0,
    base_delay=2.0,
    exceptions=(ConnectionError, TimeoutError),
)

@custom_retry
def my_function():
    pass
```

## 🚀 部署

### Docker 部署

```bash
# 构建镜像
docker build -t chroma-ollama .

# 运行容器
docker run -d \
  --name chroma-ollama \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/chroma_db:/app/chroma_db \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  chroma-ollama
```

### Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chroma-ollama
spec:
  replicas: 1
  selector:
    matchLabels:
      app: chroma-ollama
  template:
    metadata:
      labels:
        app: chroma-ollama
    spec:
      containers:
      - name: chroma-ollama
        image: chroma-ollama:latest
        env:
        - name: OLLAMA_BASE_URL
          value: "http://ollama-service:11434"
        volumeMounts:
        - name: data
          mountPath: /app/data
        - name: chroma-db
          mountPath: /app/chroma_db
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data-pvc
      - name: chroma-db
        persistentVolumeClaim:
          claimName: chroma-db-pvc
```

## 📈 性能基准

### 文档摄入性能
- 小文件 (< 1MB): ~1000 文档/秒
- 大文件 (> 10MB): ~500 文档/秒
- 批处理大小: 100-500 文档/批次

### 搜索性能
- 查询响应时间: < 100ms
- 支持并发查询
- 结果相关性排序

## 🤝 贡献

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📚 详细文档

- [📋 Kafka文件处理完整指南](KAFKA_COMPLETE_GUIDE.md) - 从任务生成到结果通知的完整解决方案

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🆘 故障排除

### 常见问题

1. **Ollama 连接失败**
   - 检查 Ollama 服务是否运行
   - 验证 `OLLAMA_BASE_URL` 配置
   - 确认网络连接

2. **ChromaDB 权限错误**
   - 检查数据库目录权限
   - 确保有写入权限

3. **Kafka 连接问题**
   - 验证 Kafka 集群地址
   - 检查网络连接和防火墙设置

### 调试模式

```bash
# 启用调试日志
export LOG_LEVEL=DEBUG
python -m chroma_ollama.ingest
```

## 📞 支持

如有问题或建议，请：
1. 查看 [Issues](../../issues)
2. 创建新的 Issue
3. 联系维护者
