"""配置管理模块

使用Pydantic提供类型安全的配置管理，支持环境变量和.env文件。
"""

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict


class ChromaConfig(BaseModel):
    """ChromaDB配置"""
    model_config = ConfigDict(
        populate_by_name=True,
        extra='forbid',
        str_strip_whitespace=True
    )
    
    db_dir: Path = Field(
        default=Path("./chroma_db"), 
        description="ChromaDB存储目录", 
        validation_alias="CHROMA_DB_DIR"
    )
    collection: str = Field(
        default="docs", 
        description="集合名称", 
        validation_alias="CHROMA_COLLECTION"
    )
    embedding_model: str = Field(
        default="nomic-embed-text", 
        description="嵌入模型名称", 
        validation_alias="OLLAMA_EMBED_MODEL"
    )
    ollama_base_url: str = Field(
        default="http://localhost:11434", 
        description="Ollama服务地址", 
        validation_alias="OLLAMA_BASE_URL"
    )
    
    @field_validator("ollama_base_url")
    @classmethod
    def validate_ollama_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("Ollama base URL must start with http:// or https://")
        return v


class KafkaConfig(BaseModel):
    """Kafka配置"""
    model_config = ConfigDict(
        populate_by_name=True,
        extra='forbid',
        str_strip_whitespace=True
    )
    
    bootstrap_servers: str = Field(
        description="Kafka集群地址", 
        validation_alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    topic: str = Field(
        default="obs-file-tasks", 
        description="任务主题名", 
        validation_alias="KAFKA_TOPIC_TASKS"
    )
    group_id: str = Field(
        default="obs-worker-group", 
        description="消费组ID", 
        validation_alias="KAFKA_GROUP_ID"
    )
    security_protocol: str = Field(
        default="PLAINTEXT", 
        description="安全协议", 
        validation_alias="KAFKA_SECURITY_PROTOCOL"
    )
    sasl_mechanism: Optional[str] = Field(
        default=None, 
        description="SASL机制", 
        validation_alias="KAFKA_SASL_MECHANISM"
    )
    sasl_username: Optional[str] = Field(
        default=None, 
        description="SASL用户名", 
        validation_alias="KAFKA_SASL_USERNAME"
    )
    sasl_password: Optional[str] = Field(
        default=None, 
        description="SASL密码", 
        validation_alias="KAFKA_SASL_PASSWORD"
    )
    
    # 2025年最新的Kafka客户端配置
    auto_offset_reset: str = Field(
        default="earliest", 
        description="自动偏移重置策略", 
        validation_alias="KAFKA_AUTO_OFFSET_RESET"
    )
    enable_auto_commit: bool = Field(
        default=True, 
        description="是否启用自动提交", 
        validation_alias="KAFKA_ENABLE_AUTO_COMMIT"
    )
    auto_commit_interval_ms: int = Field(
        default=5000, 
        description="自动提交间隔(毫秒)", 
        validation_alias="KAFKA_AUTO_COMMIT_INTERVAL_MS"
    )
    session_timeout_ms: int = Field(
        default=30000, 
        description="会话超时(毫秒)", 
        validation_alias="KAFKA_SESSION_TIMEOUT_MS"
    )
    heartbeat_interval_ms: int = Field(
        default=10000, 
        description="心跳间隔(毫秒)", 
        validation_alias="KAFKA_HEARTBEAT_INTERVAL_MS"
    )
    max_poll_interval_ms: int = Field(
        default=300000, 
        description="最大轮询间隔(毫秒)", 
        validation_alias="KAFKA_MAX_POLL_INTERVAL_MS"
    )
    max_poll_records: int = Field(
        default=500, 
        description="每次轮询最大记录数", 
        validation_alias="KAFKA_MAX_POLL_RECORDS"
    )
    
    # 生产者配置
    result_topic: str = Field(
        default="file-processing-results",
        description="处理结果通知Topic",
        validation_alias="KAFKA_RESULT_TOPIC"
    )
    enable_producer: bool = Field(
        default=True,
        description="是否启用Kafka生产者发送结果通知",
        validation_alias="KAFKA_ENABLE_PRODUCER"
    )
    
    @field_validator("security_protocol")
    @classmethod
    def validate_security_protocol(cls, v):
        valid_protocols = {"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"}
        if v not in valid_protocols:
            raise ValueError(f"Security protocol must be one of {valid_protocols}")
        return v


class OBSConfig(BaseModel):
    """OBS配置"""
    model_config = ConfigDict(
        populate_by_name=True,
        extra='forbid',
        str_strip_whitespace=True
    )
    
    access_key: str = Field(
        description="访问密钥", 
        validation_alias="OBS_ACCESS_KEY"
    )
    secret_key: str = Field(
        description="密钥", 
        validation_alias="OBS_SECRET_KEY"
    )
    server: str = Field(
        description="OBS服务器地址", 
        validation_alias="OBS_SERVER"
    )
    region: str = Field(
        description="区域代号", 
        validation_alias="OBS_REGION"
    )
    source_bucket: str = Field(
        description="源桶名称", 
        validation_alias="OBS_SOURCE_BUCKET"
    )
    dest_bucket: str = Field(
        description="目标桶名称", 
        validation_alias="OBS_DEST_BUCKET"
    )
    download_dir: Path = Field(
        default=Path("./work/downloads"), 
        description="本地下载目录", 
        validation_alias="OBS_DOWNLOAD_DIR"
    )
    upload_dir: Path = Field(
        default=Path("./work/outputs"), 
        description="本地输出目录", 
        validation_alias="OBS_UPLOAD_DIR"
    )
    
    @field_validator("server")
    @classmethod
    def validate_server_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("OBS server URL must start with http:// or https://")
        return v


class WorkerConfig(BaseModel):
    """Worker配置"""
    model_config = ConfigDict(
        populate_by_name=True,
        extra='forbid',
        str_strip_whitespace=True
    )
    
    pool_size: int = Field(
        default=4, 
        ge=1, 
        le=32, 
        description="进程池大小", 
        validation_alias="POOL_SIZE"
    )
    dbc_factory: Optional[str] = Field(
        default=None, 
        description="DBC解析器工厂", 
        validation_alias="DBC_FACTORY"
    )
    
    # 文件清理配置
    cleanup_files: bool = Field(
        default=True,
        description="处理完成后是否清理本地文件",
        validation_alias="CLEANUP_FILES"
    )
    cleanup_on_error: bool = Field(
        default=False,
        description="处理失败时是否清理本地文件",
        validation_alias="CLEANUP_ON_ERROR"
    )


class Config(BaseSettings):
    """主配置类"""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # 子配置
    chroma: ChromaConfig = Field(default_factory=ChromaConfig)
    kafka: Optional[KafkaConfig] = Field(default=None)
    obs: Optional[OBSConfig] = Field(default=None)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    
    @classmethod
    def get_default_config(cls) -> "Config":
        """获取带有默认值的配置实例（用于测试）"""
        return cls(
            kafka=KafkaConfig(
                bootstrap_servers="localhost:9092",
                topic="test-tasks",
                group_id="test-group",
                result_topic="test-results"
            ),
            obs=OBSConfig(
                access_key="test-key",
                secret_key="test-secret",
                server="https://test.obs.com",
                region="test-region",
                source_bucket="test-source",
                dest_bucket="test-dest"
            )
        )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 确保目录存在
        self.chroma.db_dir.mkdir(parents=True, exist_ok=True)
        if self.obs:
            self.obs.download_dir.mkdir(parents=True, exist_ok=True)
            self.obs.upload_dir.mkdir(parents=True, exist_ok=True)


# 全局配置实例
config = Config()
