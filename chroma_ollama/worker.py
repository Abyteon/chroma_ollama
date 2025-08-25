"""改进的Kafka Worker模块

使用新的配置管理、日志系统和重试机制，提供更好的错误处理和监控。
"""

import json
import signal
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import orjson
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from confluent_kafka.cimpl import Message

from .config import config
from .logging import get_logger
from .retry import retry_on_kafka_error, retry_on_obs_error
from .utils import ObsHelper, KafkaProducerHelper, ensure_dirs, get_file_size, format_file_size, cleanup_files

logger = get_logger(__name__)


class KafkaWorker:
    """Kafka驱动的OBS文件处理器"""
    
    def __init__(self):
        """初始化Worker"""
        if not config.kafka:
            raise ValueError("Kafka配置未提供")
        if not config.obs:
            raise ValueError("OBS配置未提供")
        
        self.kafka_config = config.kafka
        self.obs_config = config.obs
        self.worker_config = config.worker
        self.obs_helper = ObsHelper()
        self.kafka_producer = KafkaProducerHelper() if self.kafka_config.enable_producer else None
        self.consumer = None
        self.running = False
        
        # 确保目录存在
        ensure_dirs(self.obs_config.download_dir, self.obs_config.upload_dir)
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在优雅关闭...")
        self.running = False
    
    def _build_consumer(self) -> ConfluentConsumer:
        """构建Kafka消费者
        
        Returns:
            配置好的Kafka消费者
        """
        consumer_config = {
            "bootstrap.servers": self.kafka_config.bootstrap_servers,
            "group.id": self.kafka_config.group_id,
            "auto.offset.reset": self.kafka_config.auto_offset_reset,
            "enable.auto.commit": self.kafka_config.enable_auto_commit,
            "auto.commit.interval.ms": self.kafka_config.auto_commit_interval_ms,
            "session.timeout.ms": self.kafka_config.session_timeout_ms,
            "heartbeat.interval.ms": self.kafka_config.heartbeat_interval_ms,
            "max.poll.interval.ms": self.kafka_config.max_poll_interval_ms,
            "max.poll.records": self.kafka_config.max_poll_records,
        }
        
        # 可选的安全配置
        if self.kafka_config.security_protocol != "PLAINTEXT":
            consumer_config.update({
                "security.protocol": self.kafka_config.security_protocol,
                "sasl.mechanisms": self.kafka_config.sasl_mechanism or "PLAIN",
                "sasl.username": self.kafka_config.sasl_username or "",
                "sasl.password": self.kafka_config.sasl_password or "",
            })
        
        return ConfluentConsumer(consumer_config)
    
    def _parse_message(self, message: Message) -> Dict[str, Any]:
        """解析Kafka消息
        
        Args:
            message: Kafka消息对象
            
        Returns:
            解析后的消息内容
            
        Raises:
            ValueError: 消息格式无效时抛出
        """
        if message.error():
            raise ValueError(f"Kafka消息错误: {message.error()}")
        
        try:
            # 尝试解析为JSON
            if isinstance(message.value(), (bytes, bytearray)):
                payload = orjson.loads(message.value())
            else:
                payload = message.value()
        except Exception as e:
            logger.error(f"消息解析失败: {e}")
            raise ValueError(f"无效的消息格式: {e}")
        
        # 验证必需字段
        if "key" not in payload:
            raise ValueError("消息缺少必需的 'key' 字段")
        
        return payload
    
    @retry_on_obs_error
    def _download_file(self, bucket: str, key: str, local_path: Path) -> None:
        """下载OBS文件
        
        Args:
            bucket: 源桶名称
            key: 对象键
            local_path: 本地文件路径
        """
        logger.info(f"下载文件: obs://{bucket}/{key} -> {local_path}")
        self.obs_helper.download(bucket, key, local_path)
    
    def _parse_file(self, local_input: Path, local_output: Path) -> None:
        """解析文件
        
        Args:
            local_input: 输入文件路径
            local_output: 输出文件路径
        """
        logger.info(f"解析文件: {local_input} -> {local_output}")
        
        # 确保输出目录存在
        local_output.parent.mkdir(parents=True, exist_ok=True)
        
        # 这里可以替换为实际的解析逻辑
        file_size = get_file_size(local_input)
        content = {
            "input_file": str(local_input),
            "size_bytes": file_size,
            "size_formatted": format_file_size(file_size),
            "processed_at": str(Path().cwd()),
        }
        
        local_output.write_bytes(orjson.dumps(content))
    
    @retry_on_obs_error
    def _upload_file(self, bucket: str, key: str, local_path: Path) -> None:
        """上传文件到OBS
        
        Args:
            bucket: 目标桶名称
            key: 对象键
            local_path: 本地文件路径
        """
        logger.info(f"上传文件: {local_path} -> obs://{bucket}/{key}")
        self.obs_helper.upload(bucket, key, local_path)
    
    def _process_message(self, payload: Dict[str, Any]) -> None:
        """处理单个消息
        
        Args:
            payload: 消息内容
        """
        # 提取消息参数
        source_bucket = payload.get("bucket") or self.obs_config.source_bucket
        obj_key = payload["key"]
        dest_bucket = payload.get("dest_bucket") or self.obs_config.dest_bucket
        dest_key = payload.get("dest_key") or f"{obj_key}.parsed.json"
        
        # 构建本地路径
        local_input = self.obs_config.download_dir / obj_key
        local_output = self.obs_config.upload_dir / dest_key
        
        processing_success = False
        
        try:
            # 下载文件
            self._download_file(source_bucket, obj_key, local_input)
            
            # 解析文件
            self._parse_file(local_input, local_output)
            
            # 上传结果
            self._upload_file(dest_bucket, dest_key, local_output)
            
            processing_success = True
            logger.info(f"处理完成: {obj_key}")
            
            # 发送成功通知
            self._send_result_notification(
                obj_key=obj_key,
                status="success",
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                local_input=local_input,
                local_output=local_output
            )
            
        except Exception as e:
            logger.error(
                f"处理失败: {obj_key}",
                error=str(e),
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
            )
            
            # 发送失败通知
            self._send_result_notification(
                obj_key=obj_key,
                status="failed",
                error=str(e),
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
                local_input=local_input,
                local_output=local_output
            )
            
            raise
        finally:
            # 清理本地文件
            self._cleanup_local_files(
                local_input, 
                local_output, 
                success=processing_success
            )
    
    def _consume_messages(self) -> None:
        """消费Kafka消息"""
        self.consumer = self._build_consumer()
        self.consumer.subscribe([self.kafka_config.topic])
        
        logger.info(
            "Worker启动",
            topic=self.kafka_config.topic,
            group_id=self.kafka_config.group_id,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
        )
        
        self.running = True
        
        try:
            while self.running:
                # 轮询消息
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                try:
                    # 解析消息
                    payload = self._parse_message(msg)
                    
                    # 处理消息
                    self._process_message(payload)
                    
                    # 手动提交偏移量（可选）
                    # self.consumer.commit(msg)
                    
                except Exception as e:
                    logger.error(f"消息处理失败: {e}")
                    # 可以选择跳过此消息或重试
                    continue
                    
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭...")
        except Exception as e:
            logger.error(f"消费过程中发生错误: {e}")
            raise
        finally:
            self._cleanup()
    
    def _send_result_notification(
        self, 
        obj_key: str, 
        status: str, 
        source_bucket: str,
        dest_bucket: str,
        dest_key: str,
        local_input: Path,
        local_output: Path,
        error: Optional[str] = None
    ) -> None:
        """发送处理结果通知
        
        Args:
            obj_key: 原始文件键
            status: 处理状态 (success/failed)
            source_bucket: 源桶
            dest_bucket: 目标桶
            dest_key: 目标文件键
            local_input: 本地输入文件路径
            local_output: 本地输出文件路径
            error: 错误信息（失败时）
        """
        if not self.kafka_producer:
            return
            
        try:
            import datetime
            
            # 构建通知消息
            notification = {
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "worker_type": "single_process",
                "status": status,
                "source": {
                    "bucket": source_bucket,
                    "key": obj_key,
                },
                "destination": {
                    "bucket": dest_bucket,
                    "key": dest_key,
                },
                "file_info": {
                    "input_size": get_file_size(local_input),
                    "output_size": get_file_size(local_output) if local_output.exists() else 0,
                    "input_size_formatted": format_file_size(get_file_size(local_input)),
                    "output_size_formatted": format_file_size(get_file_size(local_output) if local_output.exists() else 0),
                }
            }
            
            if error:
                notification["error"] = error
                
            # 发送通知
            self.kafka_producer.send_notification(
                topic=self.kafka_config.result_topic,
                message=notification,
                key=obj_key
            )
            
            logger.debug(f"处理结果通知已发送: {obj_key} -> {status}")
            
        except Exception as e:
            logger.warning(f"发送结果通知失败: {e}")
    
    def _cleanup_local_files(
        self, 
        local_input: Path, 
        local_output: Path, 
        success: bool
    ) -> None:
        """清理本地文件
        
        Args:
            local_input: 本地输入文件路径
            local_output: 本地输出文件路径
            success: 处理是否成功
        """
        if not self.worker_config.cleanup_files:
            logger.debug("文件清理已禁用")
            return
            
        # 根据配置决定是否清理
        should_cleanup = success or self.worker_config.cleanup_on_error
        
        if should_cleanup:
            try:
                cleanup_success = cleanup_files(
                    local_input, 
                    local_output, 
                    force=self.worker_config.cleanup_on_error
                )
                
                if cleanup_success:
                    logger.debug(f"本地文件清理成功: {local_input.name}, {local_output.name}")
                else:
                    logger.warning(f"本地文件清理失败: {local_input.name}, {local_output.name}")
                    
            except Exception as e:
                logger.error(f"文件清理过程中发生错误: {e}")
        else:
            logger.debug(f"保留本地文件（处理失败且未配置清理）: {local_input.name}, {local_output.name}")
    
    def _cleanup(self) -> None:
        """清理资源"""
        if self.consumer:
            logger.info("关闭Kafka消费者...")
            self.consumer.close()
        
        if self.kafka_producer:
            logger.info("关闭Kafka生产者...")
            self.kafka_producer.close()
    
    def start(self) -> None:
        """启动Worker"""
        try:
            self._consume_messages()
        except Exception as e:
            logger.error(f"Worker启动失败: {e}")
            raise


def main() -> None:
    """主函数"""
    try:
        worker = KafkaWorker()
        logger.info("Kafka Worker 已启动")
        worker.start()
    except Exception as e:
        logger.error(f"Worker运行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
