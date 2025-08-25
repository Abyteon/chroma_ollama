"""重试机制模块

使用tenacity库提供强大的重试功能，支持指数退避、条件重试等。
"""

import time
from functools import wraps
from typing import Any, Callable, Optional, Type, Union

from tenacity import (
    RetryCallState,
    RetryError,
    Retrying,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
    retry_if_exception_type,
    retry_if_result,
    before_sleep_log,
    after_log,
)
from tenacity.wait import wait_base

from .logging import get_logger

logger = get_logger(__name__)


def create_retry_decorator(
    max_attempts: int = 3,
    max_delay: float = 60.0,
    base_delay: float = 1.0,
    exceptions: Union[Type[Exception], tuple[Type[Exception], ...]] = Exception,
    retry_on_result: Optional[Callable[[Any], bool]] = None,
    before_retry: Optional[Callable[[RetryCallState], None]] = None,
    after_retry: Optional[Callable[[RetryCallState], None]] = None,
) -> Callable:
    """创建重试装饰器
    
    Args:
        max_attempts: 最大重试次数
        max_delay: 最大延迟时间（秒）
        base_delay: 基础延迟时间（秒）
        exceptions: 需要重试的异常类型
        retry_on_result: 基于返回值决定是否重试的函数
        before_retry: 重试前的回调函数
        after_retry: 重试后的回调函数
        
    Returns:
        重试装饰器
    """
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_strategy = Retrying(
                stop=stop_after_attempt(max_attempts) | stop_after_delay(max_delay),
                wait=wait_exponential(multiplier=base_delay, max=max_delay),
                retry=retry_if_exception_type(exceptions),
                before_sleep=before_sleep_log(logger, logger.level),
                after=after_log(logger, logger.level),
            )
            
            if retry_on_result:
                retry_strategy.retry = retry_strategy.retry | retry_if_result(retry_on_result)
            
            if before_retry:
                retry_strategy.before_sleep = before_retry
            
            if after_retry:
                retry_strategy.after = after_retry
            
            try:
                return retry_strategy(func, *args, **kwargs)
            except RetryError as e:
                logger.error(
                    "Function failed after all retry attempts",
                    function=func.__name__,
                    attempts=max_attempts,
                    last_exception=str(e.last_attempt.exception()),
                )
                raise e.last_attempt.exception() from e
        
        return wrapper
    
    return decorator


# 预定义的重试装饰器
retry_on_network_error = create_retry_decorator(
    max_attempts=3,
    max_delay=30.0,
    base_delay=1.0,
    exceptions=(ConnectionError, TimeoutError, OSError),
)

retry_on_kafka_error = create_retry_decorator(
    max_attempts=5,
    max_delay=60.0,
    base_delay=2.0,
    exceptions=(Exception,),  # Kafka异常类型较多，使用通用异常
)

retry_on_obs_error = create_retry_decorator(
    max_attempts=3,
    max_delay=30.0,
    base_delay=1.0,
    exceptions=(Exception,),  # OBS SDK异常类型较多
)

retry_on_ollama_error = create_retry_decorator(
    max_attempts=3,
    max_delay=10.0,
    base_delay=0.5,
    exceptions=(ConnectionError, TimeoutError, OSError),
)


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Union[Type[Exception], tuple[Type[Exception], ...]] = Exception,
) -> Any:
    """带指数退避的重试函数
    
    Args:
        func: 要执行的函数
        max_attempts: 最大重试次数
        base_delay: 基础延迟时间（秒）
        max_delay: 最大延迟时间（秒）
        exceptions: 需要重试的异常类型
        
    Returns:
        函数执行结果
        
    Raises:
        最后一次尝试的异常
    """
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt == max_attempts - 1:
                logger.error(
                    "Function failed after all retry attempts",
                    function=func.__name__,
                    attempts=max_attempts,
                    last_exception=str(e),
                )
                raise e
            
            # 计算延迟时间（指数退避）
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.warning(
                "Function failed, retrying",
                function=func.__name__,
                attempt=attempt + 1,
                max_attempts=max_attempts,
                delay=delay,
                exception=str(e),
            )
            time.sleep(delay)
    
    # 这行代码理论上不会执行到
    raise last_exception
