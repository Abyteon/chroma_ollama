"""工具模块

包含OBS操作助手和其他实用工具函数。
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import orjson
from confluent_kafka import Producer
from obs import ObsClient
from obs import PutObjectHeader

from .config import config
from .logging import get_logger
from .retry import retry_on_obs_error, retry_on_kafka_error

logger = get_logger(__name__)


class KafkaProducerHelper:
    """Kafka生产者助手类"""
    
    def __init__(self, kafka_config: Optional[Any] = None):
        """初始化Kafka生产者助手
        
        Args:
            kafka_config: Kafka配置，如果为None则使用全局配置
        """
        if kafka_config is None:
            if not config.kafka:
                raise ValueError("Kafka配置未提供")
            kafka_config = config.kafka
            
        self.config = kafka_config
        self.producer = None
        
        if self.config.enable_producer:
            self.producer = self._create_producer()
    
    def _create_producer(self) -> Producer:
        """创建Kafka生产者
        
        Returns:
            Kafka生产者实例
        """
        producer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": "all",  # 等待所有副本确认
            "retries": 3,
            "batch.size": 16384,
            "linger.ms": 5,
            "buffer.memory": 33554432,
        }
        
        # 可选的安全配置
        if self.config.security_protocol != "PLAINTEXT":
            producer_config.update({
                "security.protocol": self.config.security_protocol,
                "sasl.mechanisms": self.config.sasl_mechanism or "PLAIN",
                "sasl.username": self.config.sasl_username or "",
                "sasl.password": self.config.sasl_password or "",
            })
        
        return Producer(producer_config)
    
    @retry_on_kafka_error
    def send_notification(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> None:
        """发送通知消息
        
        Args:
            topic: 主题名称
            message: 消息内容
            key: 消息键（可选）
        """
        if not self.producer:
            logger.debug("Kafka生产者未启用，跳过消息发送")
            return
            
        try:
            # 序列化消息
            message_bytes = orjson.dumps(message)
            key_bytes = key.encode('utf-8') if key else None
            
            # 发送消息
            self.producer.produce(
                topic=topic,
                value=message_bytes,
                key=key_bytes,
                callback=self._delivery_callback
            )
            
            # 立即刷新以确保发送
            self.producer.flush(timeout=10)
            
            logger.debug(f"Kafka消息已发送: topic={topic}, key={key}")
            
        except Exception as e:
            logger.error(f"Kafka消息发送失败: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """消息投递回调
        
        Args:
            err: 错误信息
            msg: 消息对象
        """
        if err:
            logger.error(f"消息投递失败: {err}")
        else:
            logger.debug(f"消息投递成功: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
    
    def close(self) -> None:
        """关闭Kafka生产者"""
        if self.producer:
            self.producer.flush()
            logger.debug("Kafka生产者已关闭")


class ObsHelper:
    """OBS操作助手类"""
    
    def __init__(self, obs_config: Optional[Any] = None):
        """初始化OBS助手
        
        Args:
            obs_config: OBS配置，如果为None则使用全局配置
        """
        if obs_config is None:
            if not config.obs:
                raise ValueError("OBS配置未提供")
            obs_config = config.obs
        
        self.config = obs_config
        self.client = self._create_client()
    
    def _create_client(self) -> ObsClient:
        """创建OBS客户端
        
        Returns:
            OBS客户端实例
        """
        try:
            client = ObsClient(
                access_key_id=self.config.access_key,
                secret_access_key=self.config.secret_key,
                server=self.config.server,
                region=self.config.region,
            )
            
            # 测试连接
            resp = client.headBucket(self.config.source_bucket)
            if resp.status < 300:
                logger.info(f"OBS连接成功: {self.config.server}")
            else:
                logger.warning(f"OBS连接警告: {resp.status}")
            
            return client
            
        except Exception as e:
            logger.error(f"OBS客户端创建失败: {e}")
            raise
    
    @retry_on_obs_error
    def download(self, bucket: str, key: str, local_path: Path) -> None:
        """下载OBS对象到本地
        
        Args:
            bucket: 桶名称
            key: 对象键
            local_path: 本地文件路径
        """
        # 确保目录存在
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"开始下载: obs://{bucket}/{key} -> {local_path}")
        
        resp = self.client.getObject(bucket, key, downloadPath=str(local_path))
        
        if resp.status < 300:
            logger.debug(f"下载成功: {local_path}")
        else:
            raise RuntimeError(f"下载失败: {resp.status} - {resp.errorMessage}")
    
    @retry_on_obs_error
    def upload(self, bucket: str, key: str, local_path: Path) -> None:
        """上传本地文件到OBS
        
        Args:
            bucket: 桶名称
            key: 对象键
            local_path: 本地文件路径
        """
        if not local_path.exists():
            raise FileNotFoundError(f"本地文件不存在: {local_path}")
        
        logger.debug(f"开始上传: {local_path} -> obs://{bucket}/{key}")
        
        # 设置上传头信息
        headers = PutObjectHeader()
        headers.contentType = self._get_content_type(local_path)
        
        resp = self.client.putFile(bucket, key, str(local_path), headers=headers)
        
        if resp.status < 300:
            logger.debug(f"上传成功: obs://{bucket}/{key}")
        else:
            raise RuntimeError(f"上传失败: {resp.status} - {resp.errorMessage}")
    
    @retry_on_obs_error
    def list_objects(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> list:
        """列出OBS对象
        
        Args:
            bucket: 桶名称
            prefix: 前缀过滤
            max_keys: 最大返回数量
            
        Returns:
            对象列表
        """
        logger.debug(f"列出对象: obs://{bucket}/{prefix}")
        
        resp = self.client.listObjects(bucket, prefix=prefix, maxKeys=max_keys)
        
        if resp.status < 300:
            objects = []
            for obj in resp.body.contents:
                objects.append({
                    'key': obj.key,
                    'size': obj.size,
                    'lastModified': obj.lastModified,
                })
            logger.debug(f"列出对象成功: {len(objects)} 个对象")
            return objects
        else:
            raise RuntimeError(f"列出对象失败: {resp.status} - {resp.errorMessage}")
    
    @retry_on_obs_error
    def delete_object(self, bucket: str, key: str) -> None:
        """删除OBS对象
        
        Args:
            bucket: 桶名称
            key: 对象键
        """
        logger.debug(f"删除对象: obs://{bucket}/{key}")
        
        resp = self.client.deleteObject(bucket, key)
        
        if resp.status < 300:
            logger.debug(f"删除对象成功: obs://{bucket}/{key}")
        else:
            raise RuntimeError(f"删除对象失败: {resp.status} - {resp.errorMessage}")
    
    @retry_on_obs_error
    def object_exists(self, bucket: str, key: str) -> bool:
        """检查对象是否存在
        
        Args:
            bucket: 桶名称
            key: 对象键
            
        Returns:
            对象是否存在
        """
        resp = self.client.headObject(bucket, key)
        return resp.status < 300
    
    def _get_content_type(self, file_path: Path) -> str:
        """根据文件扩展名获取内容类型
        
        Args:
            file_path: 文件路径
            
        Returns:
            内容类型
        """
        suffix = file_path.suffix.lower()
        
        content_types = {
            '.json': 'application/json',
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.xml': 'application/xml',
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.pdf': 'application/pdf',
            '.zip': 'application/zip',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip',
            '.parquet': 'application/octet-stream',
            '.bin': 'application/octet-stream',
        }
        
        return content_types.get(suffix, 'application/octet-stream')
    
    def close(self) -> None:
        """关闭OBS客户端"""
        if self.client:
            self.client.close()
            logger.debug("OBS客户端已关闭")


def ensure_dirs(*dirs: Path) -> None:
    """确保目录存在
    
    Args:
        *dirs: 目录路径列表
    """
    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"确保目录存在: {dir_path}")


def get_file_size(file_path: Path) -> int:
    """获取文件大小
    
    Args:
        file_path: 文件路径
        
    Returns:
        文件大小（字节）
    """
    try:
        return file_path.stat().st_size
    except OSError:
        return 0


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小
    
    Args:
        size_bytes: 文件大小（字节）
        
    Returns:
        格式化的文件大小字符串
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


def safe_filename(filename: str) -> str:
    """生成安全的文件名
    
    Args:
        filename: 原始文件名
        
    Returns:
        安全的文件名
    """
    # 替换不安全的字符
    unsafe_chars = '<>:"/\\|?*'
    for char in unsafe_chars:
        filename = filename.replace(char, '_')
    
    # 限制长度
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255-len(ext)] + ext
    
    return filename


def cleanup_file(file_path: Path, force: bool = False) -> bool:
    """清理单个文件
    
    Args:
        file_path: 文件路径
        force: 是否强制删除（忽略错误）
        
    Returns:
        是否成功删除
    """
    try:
        if file_path.exists():
            file_path.unlink()
            logger.debug(f"文件已删除: {file_path}")
            return True
        else:
            logger.debug(f"文件不存在，无需删除: {file_path}")
            return True
    except Exception as e:
        if force:
            logger.warning(f"文件删除失败但被忽略: {file_path}, 错误: {e}")
            return True
        else:
            logger.error(f"文件删除失败: {file_path}, 错误: {e}")
            return False


def cleanup_files(*file_paths: Path, force: bool = False) -> bool:
    """批量清理文件
    
    Args:
        *file_paths: 文件路径列表
        force: 是否强制删除（忽略错误）
        
    Returns:
        是否全部成功删除
    """
    success_count = 0
    total_count = len(file_paths)
    
    for file_path in file_paths:
        if cleanup_file(file_path, force=force):
            success_count += 1
    
    if success_count == total_count:
        logger.debug(f"批量文件清理成功: {success_count}/{total_count}")
        return True
    else:
        logger.warning(f"批量文件清理部分失败: {success_count}/{total_count}")
        return not force  # 如果force=True则仍返回True


def cleanup_directory(dir_path: Path, pattern: str = "*", force: bool = False) -> bool:
    """清理目录中的文件
    
    Args:
        dir_path: 目录路径
        pattern: 文件匹配模式，默认为所有文件
        force: 是否强制删除（忽略错误）
        
    Returns:
        是否成功清理
    """
    try:
        if not dir_path.exists():
            logger.debug(f"目录不存在，无需清理: {dir_path}")
            return True
            
        files_to_delete = list(dir_path.glob(pattern))
        if not files_to_delete:
            logger.debug(f"目录中没有匹配的文件: {dir_path}/{pattern}")
            return True
            
        return cleanup_files(*files_to_delete, force=force)
        
    except Exception as e:
        if force:
            logger.warning(f"目录清理失败但被忽略: {dir_path}, 错误: {e}")
            return True
        else:
            logger.error(f"目录清理失败: {dir_path}, 错误: {e}")
            return False
