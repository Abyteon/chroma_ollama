"""Chroma + Ollama 向量库项目

一个基于ChromaDB和Ollama的本地向量搜索解决方案，
支持JSON文档的向量化和语义搜索，以及Kafka驱动的文件处理。
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .config import Config
from .utils import ObsHelper

__all__ = ["Config", "ObsHelper"]
