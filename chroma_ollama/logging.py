"""日志配置模块

使用structlog提供结构化日志，支持JSON格式输出和性能优化。
"""

import sys
from typing import Any, Dict

import structlog
from structlog.types import Processor


def configure_logging(
    level: str = "INFO",
    json_format: bool = True,
    include_timestamp: bool = True,
    include_caller: bool = True,
) -> None:
    """配置结构化日志系统
    
    Args:
        level: 日志级别
        json_format: 是否使用JSON格式输出
        include_timestamp: 是否包含时间戳
        include_caller: 是否包含调用者信息
    """
    
    # 配置标准库日志
    import logging
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    
    # 定义处理器
    processors: list[Processor] = [
        structlog.stdlib.filter_by_level,
    ]
    
    if include_timestamp:
        processors.append(structlog.stdlib.add_logger_name)
        processors.append(structlog.processors.TimeStamper(fmt="iso"))
    
    if include_caller:
        processors.append(structlog.processors.CallsiteParameterAdder(
            parameters={
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ))
    
    processors.extend([
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ])
    
    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    # 配置structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """获取结构化日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        配置好的日志记录器
    """
    return structlog.get_logger(name)


# 默认配置
configure_logging()
