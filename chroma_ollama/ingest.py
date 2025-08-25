"""改进的文档摄入模块

使用新的配置管理、日志系统和重试机制。
"""

import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional

import orjson
import chromadb
from chromadb.utils.embedding_functions import OllamaEmbeddingFunction
from tqdm import tqdm

from .config import config
from .logging import get_logger
from .retry import retry_on_ollama_error

logger = get_logger(__name__)


def compute_id(text: str) -> str:
    """生成文档的确定性ID
    
    Args:
        text: 文档文本内容
        
    Returns:
        SHA256哈希值作为文档ID
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_json_documents(json_path: Path) -> List[Tuple[str, Dict[str, Any]]]:
    """从JSON文件加载文档
    
    Args:
        json_path: JSON文件路径
        
    Returns:
        文档列表，每个元素为(文本, 元数据)元组
        
    Raises:
        RuntimeError: JSON解析失败时抛出
    """
    try:
        data = orjson.loads(json_path.read_bytes())
    except Exception as exc:
        raise RuntimeError(f"Failed to parse JSON: {json_path}: {exc}")

    documents: List[Tuple[str, Dict[str, Any]]] = []

    def to_text_and_meta(item: Any) -> Tuple[str, Dict[str, Any]]:
        """将JSON项转换为文本和元数据"""
        if isinstance(item, str):
            text = item
            meta: Dict[str, Any] = {}
        elif isinstance(item, dict):
            # 优先使用文本字段
            text_field = None
            for key in ("text", "content", "body", "文本", "内容"):
                if key in item and isinstance(item[key], str) and item[key].strip():
                    text_field = key
                    break
            
            if text_field:
                text = str(item[text_field])
            else:
                # 如果没有文本字段，序列化整个对象
                text = orjson.dumps(item, option=orjson.OPT_INDENT_2).decode("utf-8")
            
            meta = {k: v for k, v in item.items() if k != text_field}
        else:
            # 其他类型序列化为JSON
            text = orjson.dumps(item, option=orjson.OPT_INDENT_2).decode("utf-8")
            meta = {}
        
        # 添加来源信息
        meta.update({
            "source_file": str(json_path),
            "source_name": json_path.name,
        })
        return text, meta

    # 支持数组、字典或基本类型的顶级JSON结构
    if isinstance(data, list):
        for elem in data:
            documents.append(to_text_and_meta(elem))
    elif isinstance(data, dict):
        # 容器模式：常见的列表键
        container_keys = ["items", "data", "documents", "docs", "记录", "数据"]
        list_found = False
        for key in container_keys:
            if key in data and isinstance(data[key], list):
                for elem in data[key]:
                    documents.append(to_text_and_meta(elem))
                list_found = True
                break
        if not list_found:
            documents.append(to_text_and_meta(data))
    else:
        documents.append(to_text_and_meta(data))

    return documents


def batched(iterable: Iterable[Any], batch_size: int) -> Iterable[List[Any]]:
    """将可迭代对象分批
    
    Args:
        iterable: 要分批的可迭代对象
        batch_size: 批次大小
        
    Yields:
        固定大小的批次列表（最后一个批次可能较小）
    """
    batch: List[Any] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


@retry_on_ollama_error
def create_embedding_function() -> OllamaEmbeddingFunction:
    """创建Ollama嵌入函数
    
    Returns:
        配置好的Ollama嵌入函数
    """
    return OllamaEmbeddingFunction(
        model=config.chroma.embedding_model,
        base_url=config.chroma.ollama_base_url,
    )


def ingest_documents(
    data_dir: Path,
    batch_size: int = 100,
    collection_name: Optional[str] = None,
) -> None:
    """摄入文档到ChromaDB
    
    Args:
        data_dir: 数据目录路径
        batch_size: 批处理大小
        collection_name: 集合名称，如果为None则使用配置中的默认值
    """
    if collection_name is None:
        collection_name = config.chroma.collection
    
    logger.info(
        "Starting document ingestion",
        data_dir=str(data_dir),
        collection=collection_name,
        batch_size=batch_size,
    )
    
    # 创建嵌入函数
    embed_fn = create_embedding_function()
    
    # 连接到ChromaDB
    client = chromadb.PersistentClient(path=str(config.chroma.db_dir))
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=embed_fn,
        metadata={"hnsw:space": "cosine"},
    )
    
    # 扫描JSON文件
    json_files = list(data_dir.rglob("*.json"))
    logger.info(f"Found {len(json_files)} JSON files")
    
    if not json_files:
        logger.warning("No JSON files found in data directory")
        return
    
    total_documents = 0
    
    # 处理每个JSON文件
    for json_file in tqdm(json_files, desc="Processing files"):
        try:
            documents = load_json_documents(json_file)
            logger.debug(
                f"Loaded {len(documents)} documents from {json_file.name}"
            )
            
            # 分批处理文档
            for batch in batched(documents, batch_size):
                texts = [doc[0] for doc in batch]
                metadatas = [doc[1] for doc in batch]
                ids = [compute_id(text) for text in texts]
                
                # 批量添加到集合
                collection.upsert(
                    documents=texts,
                    metadatas=metadatas,
                    ids=ids,
                )
                
                total_documents += len(batch)
                
        except Exception as e:
            logger.error(
                f"Failed to process {json_file}: {e}",
                file=str(json_file),
                error=str(e),
            )
            continue
    
    logger.info(
        "Document ingestion completed",
        total_files=len(json_files),
        total_documents=total_documents,
        collection=collection_name,
    )


def main() -> None:
    """主函数"""
    import os
    import sys
    
    # 从环境变量获取参数，提供默认值
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    collection_name = os.getenv("COLLECTION_NAME")
    
    # 确保数据目录存在
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    try:
        ingest_documents(
            data_dir=data_dir,
            batch_size=batch_size,
            collection_name=collection_name,
        )
        logger.info("文档摄入完成")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
