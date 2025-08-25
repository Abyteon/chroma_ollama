#!/usr/bin/env python3
"""测试改进功能的脚本"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

def test_config():
    """测试配置管理"""
    print("🔧 测试配置管理...")
    try:
        from chroma_ollama.config import config
        print(f"✅ ChromaDB目录: {config.chroma.db_dir}")
        print(f"✅ 嵌入模型: {config.chroma.embedding_model}")
        print(f"✅ Ollama URL: {config.chroma.ollama_base_url}")
        print("✅ 配置管理测试通过")
        return True
    except Exception as e:
        print(f"❌ 配置管理测试失败: {e}")
        return False

def test_logging():
    """测试日志系统"""
    print("\n📝 测试日志系统...")
    try:
        from chroma_ollama.logging import get_logger, configure_logging
        
        # 配置日志
        configure_logging(level="INFO", json_format=False)
        
        # 获取日志记录器
        logger = get_logger("test")
        
        # 测试日志输出
        logger.info("这是一条测试日志", test_field="test_value")
        logger.warning("这是一条警告日志")
        logger.error("这是一条错误日志")
        
        print("✅ 日志系统测试通过")
        return True
    except Exception as e:
        print(f"❌ 日志系统测试失败: {e}")
        return False

def test_retry():
    """测试重试机制"""
    print("\n🔄 测试重试机制...")
    try:
        from chroma_ollama.retry import retry_on_network_error, retry_with_backoff
        
        # 测试重试装饰器
        @retry_on_network_error
        def test_function():
            return "success"
        
        result = test_function()
        print(f"✅ 重试装饰器测试通过: {result}")
        
        # 测试重试函数
        def failing_function():
            raise ConnectionError("测试连接错误")
        
        try:
            retry_with_backoff(failing_function, max_attempts=2)
        except ConnectionError:
            print("✅ 重试函数测试通过（按预期失败）")
        
        return True
    except Exception as e:
        print(f"❌ 重试机制测试失败: {e}")
        return False

def test_utils():
    """测试工具函数"""
    print("\n🛠️ 测试工具函数...")
    try:
        from chroma_ollama.utils import format_file_size, safe_filename, ensure_dirs
        
        # 测试文件大小格式化
        size_str = format_file_size(1024)
        print(f"✅ 文件大小格式化: 1024 bytes = {size_str}")
        
        # 测试安全文件名
        safe_name = safe_filename("test<>file.txt")
        print(f"✅ 安全文件名: test<>file.txt -> {safe_name}")
        
        # 测试目录创建
        test_dir = Path("./test_dir")
        ensure_dirs(test_dir)
        print(f"✅ 目录创建: {test_dir}")
        
        # 清理测试目录
        test_dir.rmdir()
        
        return True
    except Exception as e:
        print(f"❌ 工具函数测试失败: {e}")
        return False

def test_imports():
    """测试模块导入"""
    print("\n📦 测试模块导入...")
    try:
        # 测试主要模块导入
        modules = [
            "chroma_ollama.config",
            "chroma_ollama.logging", 
            "chroma_ollama.retry",
            "chroma_ollama.utils",
            "chroma_ollama.ingest",
            "chroma_ollama.search",
            "chroma_ollama.worker",
            "chroma_ollama.pool_worker",
        ]
        
        for module_name in modules:
            __import__(module_name)
            print(f"✅ {module_name}")
        
        print("✅ 所有模块导入成功")
        return True
    except Exception as e:
        print(f"❌ 模块导入测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("🚀 开始测试改进功能...\n")
    
    tests = [
        test_config,
        test_logging,
        test_retry,
        test_utils,
        test_imports,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！改进功能正常工作。")
        return 0
    else:
        print("⚠️ 部分测试失败，请检查配置和依赖。")
        return 1

if __name__ == "__main__":
    sys.exit(main())
