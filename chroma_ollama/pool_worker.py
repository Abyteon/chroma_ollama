"""改进的多进程Kafka Worker模块

使用进程池和FileParser进行并行文件处理，提供更好的性能和资源管理。
"""

import importlib
import multiprocessing as mp
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import orjson
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka.cimpl import Message

from .config import config
from .logging import get_logger
from .retry import retry_on_kafka_error, retry_on_obs_error
from .utils import ObsHelper, KafkaProducerHelper, ensure_dirs, get_file_size, format_file_size, cleanup_files

logger = get_logger(__name__)


class PooledKafkaWorker:
    """多进程Kafka Worker，支持并行文件处理"""
    
    def __init__(self):
        """初始化多进程Worker"""
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
        self.executor = None
        
        # 确保目录存在
        ensure_dirs(self.obs_config.download_dir, self.obs_config.upload_dir)
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # 加载DBC解析器
        self.dbc_parser = self._load_dbc_parser()
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在优雅关闭...")
        self.running = False
    
    def _load_dbc_parser(self) -> Optional[Any]:
        """加载DBC解析器
        
        Returns:
            DBC解析器实例或None
        """
        if not self.worker_config.dbc_factory:
            logger.info("未配置DBC解析器工厂")
            return None
        
        try:
            # 解析工厂函数路径
            module_path, factory_name = self.worker_config.dbc_factory.split(":", 1)
            
            # 导入模块
            module = importlib.import_module(module_path)
            
            # 获取工厂函数
            factory_func = getattr(module, factory_name)
            
            # 创建解析器实例
            parser = factory_func()
            
            logger.info(f"成功加载DBC解析器: {self.worker_config.dbc_factory}")
            return parser
            
        except Exception as e:
            logger.error(f"加载DBC解析器失败: {e}")
            return None
    
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
    
    def _parse_file_with_pool(self, local_input: Path, local_output: Path) -> None:
        """使用进程池解析文件
        
        Args:
            local_input: 输入文件路径
            local_output: 输出文件路径
        """
        logger.info(f"使用进程池解析文件: {local_input} -> {local_output}")
        
        # 确保输出目录存在
        local_output.parent.mkdir(parents=True, exist_ok=True)
        
        # 提交解析任务到进程池
        future = self.executor.submit(self._parse_file_worker, local_input, local_output)
        
        try:
            # 等待任务完成
            result = future.result(timeout=300)  # 5分钟超时
            logger.info(f"文件解析完成: {local_input}")
        except Exception as e:
            logger.error(f"文件解析失败: {local_input}, 错误: {e}")
            raise
    
    def _parse_file_worker(self, local_input: Path, local_output: Path) -> Dict[str, Any]:
        """进程池中的文件解析工作函数
        
        Args:
            local_input: 输入文件路径
            local_output: 输出文件路径
            
        Returns:
            解析结果
        """
        try:
            # 获取文件大小
            file_size = get_file_size(local_input)
            
            # 如果有DBC解析器，使用它
            if self.dbc_parser:
                # 这里应该调用实际的DBC解析逻辑
                # 暂时使用简单的解析
                content = {
                    "input_file": str(local_input),
                    "size_bytes": file_size,
                    "size_formatted": format_file_size(file_size),
                    "parser": "dbc_parser",
                    "processed_at": str(Path().cwd()),
                }
            else:
                # 使用默认解析逻辑
                content = {
                    "input_file": str(local_input),
                    "size_bytes": file_size,
                    "size_formatted": format_file_size(file_size),
                    "parser": "default",
                    "processed_at": str(Path().cwd()),
                }
            
            # 写入结果
            local_output.write_bytes(orjson.dumps(content))
            
            return {"status": "success", "output_file": str(local_output)}
            
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
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
        
        try:
            # 下载文件
            self._download_file(source_bucket, obj_key, local_input)
            
            # 使用进程池解析文件
            self._parse_file_with_pool(local_input, local_output)
            
            # 上传结果
            self._upload_file(dest_bucket, dest_key, local_output)
            
            logger.info(f"处理完成: {obj_key}")
            
            # 清理本地文件（成功时）
            if self.worker_config.cleanup_files:
                cleanup_files(local_input, local_output, force=False)
                logger.debug(f"本地文件已清理: {local_input.name}, {local_output.name}")
            
        except Exception as e:
            logger.error(
                f"处理失败: {obj_key}",
                error=str(e),
                source_bucket=source_bucket,
                dest_bucket=dest_bucket,
            )
            
            # 清理本地文件（失败时，根据配置）
            if self.worker_config.cleanup_files and self.worker_config.cleanup_on_error:
                cleanup_files(local_input, local_output, force=True)
                logger.debug(f"本地文件已清理（失败后）: {local_input.name}, {local_output.name}")
            
            raise
    
    def _consume_messages(self) -> None:
        """消费Kafka消息"""
        self.consumer = self._build_consumer()
        self.consumer.subscribe([self.kafka_config.topic])
        
        # 创建进程池
        self.executor = ProcessPoolExecutor(
            max_workers=self.worker_config.pool_size,
            mp_context=mp.get_context('spawn')  # 使用spawn上下文，更安全
        )
        
        logger.info(
            "多进程Worker启动",
            topic=self.kafka_config.topic,
            group_id=self.kafka_config.group_id,
            pool_size=self.worker_config.pool_size,
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
    
    def _cleanup(self) -> None:
        """清理资源"""
        if self.executor:
            logger.info("关闭进程池...")
            self.executor.shutdown(wait=True)
        
        if self.consumer:
            logger.info("关闭Kafka消费者...")
            self.consumer.close()
    
    def start(self) -> None:
        """启动Worker"""
        try:
            self._consume_messages()
        except Exception as e:
            logger.error(f"Worker启动失败: {e}")
            raise


def main() -> None:
    """主函数"""
    import os
    
    # 从环境变量获取配置覆盖
    pool_size_env = os.getenv("POOL_SIZE")
    dbc_factory_env = os.getenv("DBC_FACTORY")
    
    # 覆盖配置
    if pool_size_env:
        config.worker.pool_size = int(pool_size_env)
    if dbc_factory_env:
        config.worker.dbc_factory = dbc_factory_env
    
    try:
        worker = PooledKafkaWorker()
        logger.info(f"多进程 Kafka Worker 已启动，进程池大小: {config.worker.pool_size}")
        worker.start()
    except Exception as e:
        logger.error(f"Worker运行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
