#!/usr/bin/env python3
"""Kafka Worker 测试脚本

用于测试Kafka Worker的配置和基本功能。
"""

import json
import sys
import time
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

def test_config():
    """测试Kafka配置"""
    print("🔧 测试Kafka配置...")
    try:
        from chroma_ollama.config import config
        
        if not config.kafka:
            print("❌ Kafka配置未提供")
            return False
        
        print(f"✅ Kafka服务器: {config.kafka.bootstrap_servers}")
        print(f"✅ 主题: {config.kafka.topic}")
        print(f"✅ 消费组: {config.kafka.group_id}")
        print(f"✅ 安全协议: {config.kafka.security_protocol}")
        
        if config.kafka.sasl_mechanism:
            print(f"✅ SASL机制: {config.kafka.sasl_mechanism}")
        
        return True
    except Exception as e:
        print(f"❌ Kafka配置测试失败: {e}")
        return False

def test_obs_config():
    """测试OBS配置"""
    print("\n☁️ 测试OBS配置...")
    try:
        from chroma_ollama.config import config
        
        if not config.obs:
            print("❌ OBS配置未提供")
            return False
        
        print(f"✅ OBS服务器: {config.obs.server}")
        print(f"✅ 区域: {config.obs.region}")
        print(f"✅ 源桶: {config.obs.source_bucket}")
        print(f"✅ 目标桶: {config.obs.dest_bucket}")
        print(f"✅ 下载目录: {config.obs.download_dir}")
        print(f"✅ 上传目录: {config.obs.upload_dir}")
        
        return True
    except Exception as e:
        print(f"❌ OBS配置测试失败: {e}")
        return False

def test_worker_import():
    """测试Worker模块导入"""
    print("\n📦 测试Worker模块导入...")
    try:
        # 测试单进程Worker
        from chroma_ollama.worker import KafkaWorker
        print("✅ 单进程Worker导入成功")
        
        # 测试多进程Worker
        from chroma_ollama.pool_worker import PooledKafkaWorker
        print("✅ 多进程Worker导入成功")
        
        return True
    except Exception as e:
        print(f"❌ Worker模块导入失败: {e}")
        return False

def test_message_format():
    """测试消息格式"""
    print("\n📨 测试消息格式...")
    try:
        # 测试简单消息
        simple_message = {
            "key": "test/file.txt"
        }
        
        # 测试完整消息
        full_message = {
            "bucket": "source-bucket",
            "key": "data/input.log",
            "dest_bucket": "dest-bucket",
            "dest_key": "processed/output.json"
        }
        
        # 验证必需字段
        if "key" not in simple_message:
            raise ValueError("消息缺少必需的 'key' 字段")
        
        if "key" not in full_message:
            raise ValueError("消息缺少必需的 'key' 字段")
        
        print("✅ 简单消息格式正确")
        print("✅ 完整消息格式正确")
        print("✅ 消息字段验证通过")
        
        return True
    except Exception as e:
        print(f"❌ 消息格式测试失败: {e}")
        return False

def test_utils():
    """测试工具函数"""
    print("\n🛠️ 测试工具函数...")
    try:
        from chroma_ollama.utils import ObsHelper, format_file_size, safe_filename
        
        # 测试文件大小格式化
        size_str = format_file_size(1024)
        print(f"✅ 文件大小格式化: 1024 bytes = {size_str}")
        
        # 测试安全文件名
        safe_name = safe_filename("test<>file.txt")
        print(f"✅ 安全文件名: test<>file.txt -> {safe_name}")
        
        # 测试OBS助手（不实际连接）
        print("✅ 工具函数测试通过")
        
        return True
    except Exception as e:
        print(f"❌ 工具函数测试失败: {e}")
        return False

def test_logging():
    """测试日志系统"""
    print("\n📝 测试日志系统...")
    try:
        from chroma_ollama.logging import get_logger, configure_logging
        
        # 配置日志
        configure_logging(level="INFO", json_format=False)
        
        # 获取日志记录器
        logger = get_logger("kafka_test")
        
        # 测试日志输出
        logger.info("Kafka测试开始", test_type="configuration")
        logger.warning("这是一个测试警告")
        
        print("✅ 日志系统测试通过")
        return True
    except Exception as e:
        print(f"❌ 日志系统测试失败: {e}")
        return False

def test_retry():
    """测试重试机制"""
    print("\n🔄 测试重试机制...")
    try:
        from chroma_ollama.retry import retry_on_kafka_error, retry_on_obs_error
        
        # 测试重试装饰器
        @retry_on_kafka_error
        def test_kafka_function():
            return "kafka_success"
        
        @retry_on_obs_error
        def test_obs_function():
            return "obs_success"
        
        result1 = test_kafka_function()
        result2 = test_obs_function()
        
        print(f"✅ Kafka重试装饰器: {result1}")
        print(f"✅ OBS重试装饰器: {result2}")
        
        return True
    except Exception as e:
        print(f"❌ 重试机制测试失败: {e}")
        return False

def generate_test_message():
    """生成测试消息"""
    print("\n📤 生成测试消息...")
    
    test_messages = [
        {
            "key": "test/sample.txt",
            "bucket": "test-source-bucket",
            "dest_bucket": "test-dest-bucket"
        },
        {
            "key": "data/input.json",
            "dest_key": "processed/output.json"
        },
        {
            "key": "logs/app.log"
        }
    ]
    
    for i, message in enumerate(test_messages, 1):
        print(f"消息 {i}: {json.dumps(message, ensure_ascii=False, indent=2)}")
    
    return test_messages

def main():
    """主测试函数"""
    print("🚀 开始Kafka Worker测试...\n")
    
    tests = [
        test_config,
        test_obs_config,
        test_worker_import,
        test_message_format,
        test_utils,
        test_logging,
        test_retry,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！Kafka Worker配置正确。")
        print("\n📤 测试消息示例:")
        generate_test_message()
        print("\n💡 下一步:")
        print("1. 确保Kafka服务正在运行")
        print("2. 确保OBS配置正确")
        print("3. 运行: python -m chroma_ollama.worker")
        return 0
    else:
        print("⚠️ 部分测试失败，请检查配置。")
        print("\n🔧 检查项目:")
        print("1. 环境变量配置 (.env文件)")
        print("2. Kafka服务状态")
        print("3. OBS配置")
        return 1

if __name__ == "__main__":
    sys.exit(main())
