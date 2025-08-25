"""Kafka任务生成器模块

用于生成OBS文件处理任务并发送到Kafka。
支持从文件列表、目录扫描或API调用生成任务。
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import orjson

from .config import config
from .logging import get_logger
from .utils import KafkaProducerHelper, ObsHelper
from .retry import retry_on_kafka_error, retry_on_obs_error

logger = get_logger(__name__)


class TaskGenerator:
    """Kafka任务生成器"""
    
    def __init__(self):
        """初始化任务生成器"""
        if not config.kafka:
            raise ValueError("Kafka配置未提供")
        if not config.obs:
            raise ValueError("OBS配置未提供")
            
        self.kafka_config = config.kafka
        self.obs_config = config.obs
        self.kafka_producer = KafkaProducerHelper()
        self.obs_helper = ObsHelper()
        
    def generate_task_from_obs_list(
        self, 
        bucket: str, 
        prefix: str = "", 
        dest_bucket: Optional[str] = None,
        max_files: int = 1000
    ) -> List[Dict[str, Any]]:
        """从OBS桶列表生成任务
        
        Args:
            bucket: 源桶名称
            prefix: 文件前缀过滤
            dest_bucket: 目标桶名称（可选）
            max_files: 最大文件数量
            
        Returns:
            生成的任务列表
        """
        logger.info(f"开始扫描OBS桶: {bucket}, 前缀: {prefix}")
        
        try:
            # 列出OBS对象
            objects = self.obs_helper.list_objects(
                bucket=bucket, 
                prefix=prefix, 
                max_keys=max_files
            )
            
            logger.info(f"发现 {len(objects)} 个文件")
            
            tasks = []
            for obj in objects:
                task = {
                    "key": obj["key"],
                    "bucket": bucket,
                    "size": obj["size"],
                    "last_modified": obj["lastModified"],
                    "generated_at": datetime.utcnow().isoformat()
                }
                
                # 添加目标桶（如果指定）
                if dest_bucket:
                    task["dest_bucket"] = dest_bucket
                    
                tasks.append(task)
                
            logger.info(f"生成 {len(tasks)} 个处理任务")
            return tasks
            
        except Exception as e:
            logger.error(f"从OBS生成任务失败: {e}")
            raise
    
    def generate_task_from_file_list(
        self, 
        file_list: List[str], 
        bucket: Optional[str] = None,
        dest_bucket: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """从文件列表生成任务
        
        Args:
            file_list: 文件键列表
            bucket: 源桶名称（可选，使用配置默认值）
            dest_bucket: 目标桶名称（可选）
            
        Returns:
            生成的任务列表
        """
        logger.info(f"从文件列表生成任务: {len(file_list)} 个文件")
        
        tasks = []
        for file_key in file_list:
            task = {
                "key": file_key,
                "generated_at": datetime.utcnow().isoformat()
            }
            
            # 添加源桶（如果指定）
            if bucket:
                task["bucket"] = bucket
                
            # 添加目标桶（如果指定）
            if dest_bucket:
                task["dest_bucket"] = dest_bucket
                
            tasks.append(task)
            
        logger.info(f"生成 {len(tasks)} 个处理任务")
        return tasks
    
    def generate_single_task(
        self,
        file_key: str,
        bucket: Optional[str] = None,
        dest_bucket: Optional[str] = None,
        dest_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """生成单个任务
        
        Args:
            file_key: 文件键
            bucket: 源桶名称（可选）
            dest_bucket: 目标桶名称（可选）
            dest_key: 目标文件键（可选）
            metadata: 额外的元数据（可选）
            
        Returns:
            生成的任务
        """
        task = {
            "key": file_key,
            "generated_at": datetime.utcnow().isoformat()
        }
        
        if bucket:
            task["bucket"] = bucket
        if dest_bucket:
            task["dest_bucket"] = dest_bucket
        if dest_key:
            task["dest_key"] = dest_key
        if metadata:
            task["metadata"] = metadata
            
        return task
    
    @retry_on_kafka_error
    def send_task_to_kafka(self, task: Dict[str, Any]) -> None:
        """发送单个任务到Kafka
        
        Args:
            task: 任务字典
        """
        try:
            file_key = task["key"]
            
            # 发送任务消息
            self.kafka_producer.send_notification(
                topic=self.kafka_config.topic,
                message=task,
                key=file_key
            )
            
            logger.debug(f"任务已发送到Kafka: {file_key}")
            
        except Exception as e:
            logger.error(f"发送任务到Kafka失败: {e}")
            raise
    
    def send_tasks_to_kafka(
        self, 
        tasks: List[Dict[str, Any]], 
        batch_size: int = 100,
        delay_between_batches: float = 0.1
    ) -> None:
        """批量发送任务到Kafka
        
        Args:
            tasks: 任务列表
            batch_size: 批量大小
            delay_between_batches: 批次间延迟（秒）
        """
        logger.info(f"开始发送 {len(tasks)} 个任务到Kafka")
        
        success_count = 0
        failed_count = 0
        
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info(f"发送批次 {batch_num}: {len(batch)} 个任务")
            
            for task in batch:
                try:
                    self.send_task_to_kafka(task)
                    success_count += 1
                except Exception as e:
                    logger.error(f"发送任务失败: {task.get('key', 'unknown')}, 错误: {e}")
                    failed_count += 1
            
            # 批次间延迟
            if delay_between_batches > 0 and i + batch_size < len(tasks):
                time.sleep(delay_between_batches)
        
        logger.info(f"任务发送完成: 成功 {success_count}, 失败 {failed_count}")
        
        if failed_count > 0:
            logger.warning(f"有 {failed_count} 个任务发送失败")
    
    def scan_and_generate_tasks(
        self,
        bucket: Optional[str] = None,
        prefix: str = "",
        dest_bucket: Optional[str] = None,
        max_files: int = 1000,
        send_to_kafka: bool = True
    ) -> List[Dict[str, Any]]:
        """扫描OBS并生成任务（一站式方法）
        
        Args:
            bucket: 源桶名称（可选，使用配置默认值）
            prefix: 文件前缀过滤
            dest_bucket: 目标桶名称（可选）
            max_files: 最大文件数量
            send_to_kafka: 是否自动发送到Kafka
            
        Returns:
            生成的任务列表
        """
        bucket = bucket or self.obs_config.source_bucket
        
        # 生成任务
        tasks = self.generate_task_from_obs_list(
            bucket=bucket,
            prefix=prefix,
            dest_bucket=dest_bucket,
            max_files=max_files
        )
        
        # 发送到Kafka（如果启用）
        if send_to_kafka and tasks:
            self.send_tasks_to_kafka(tasks)
        
        return tasks
    
    def close(self) -> None:
        """关闭资源"""
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.obs_helper:
            self.obs_helper.close()


def main() -> None:
    """主函数"""
    import os
    
    # 从环境变量获取参数
    bucket = os.getenv("SOURCE_BUCKET")
    prefix = os.getenv("FILE_PREFIX", "")
    dest_bucket = os.getenv("DEST_BUCKET")
    max_files = int(os.getenv("MAX_FILES", "1000"))
    
    # 支持单个文件模式
    single_file = os.getenv("SINGLE_FILE")
    
    try:
        generator = TaskGenerator()
        
        if single_file:
            # 单个文件模式
            logger.info(f"生成单个文件任务: {single_file}")
            task = generator.generate_single_task(
                file_key=single_file,
                bucket=bucket,
                dest_bucket=dest_bucket
            )
            generator.send_task_to_kafka(task)
            logger.info("单个任务已发送")
            
        else:
            # 批量扫描模式
            logger.info("开始批量任务生成...")
            tasks = generator.scan_and_generate_tasks(
                bucket=bucket,
                prefix=prefix,
                dest_bucket=dest_bucket,
                max_files=max_files,
                send_to_kafka=True
            )
            logger.info(f"批量任务生成完成: {len(tasks)} 个任务")
        
        generator.close()
        
    except Exception as e:
        logger.error(f"任务生成失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
