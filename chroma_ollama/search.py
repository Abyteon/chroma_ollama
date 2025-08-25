"""改进的搜索模块

使用新的配置管理、日志系统和重试机制。
"""

from typing import Any, Dict, List, Optional

import chromadb
from chromadb.utils.embedding_functions import OllamaEmbeddingFunction

from .config import config
from .logging import get_logger
from .retry import retry_on_ollama_error

logger = get_logger(__name__)


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


def search_documents(
    query: str,
    k: int = 5,
    collection_name: Optional[str] = None,
    include_distances: bool = True,
    include_metadatas: bool = True,
) -> Dict[str, Any]:
    """搜索文档
    
    Args:
        query: 搜索查询文本
        k: 返回结果数量
        collection_name: 集合名称，如果为None则使用配置中的默认值
        include_distances: 是否包含距离信息
        include_metadatas: 是否包含元数据
        
    Returns:
        搜索结果字典
    """
    if collection_name is None:
        collection_name = config.chroma.collection
    
    logger.info(
        "Starting document search",
        query=query,
        k=k,
        collection=collection_name,
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
    
    # 构建include参数
    include = ["documents"]
    if include_distances:
        include.append("distances")
    if include_metadatas:
        include.append("metadatas")
    
    # 执行搜索
    results = collection.query(
        query_texts=[query],
        n_results=k,
        include=include,
    )
    
    logger.info(
        "Search completed",
        query=query,
        results_count=len(results.get("documents", [[""]])[0]),
    )
    
    return results


def format_search_results(results: Dict[str, Any], query: str) -> str:
    """格式化搜索结果
    
    Args:
        results: 搜索结果字典
        query: 原始查询
        
    Returns:
        格式化的结果字符串
    """
    docs: List[str] = results.get("documents", [["No results"]])[0]
    dists: List[float] = results.get("distances", [[0.0]])[0]
    metas: List[Dict[str, Any]] = results.get("metadatas", [[{}]])[0]
    
    output = [f"Search results for: '{query}'"]
    output.append(f"Found {len(docs)} results:")
    output.append("=" * 80)
    
    for idx, (doc, dist, meta) in enumerate(zip(docs, dists, metas), start=1):
        score = 1 - dist  # 转换为相似度分数
        source = meta.get('source_name', meta.get('source_file', 'unknown'))
        
        output.append(f"[{idx}] score={score:.4f}")
        output.append(f"source={source}")
        output.append(doc[:1000])  # 限制显示长度
        output.append("-" * 80)
    
    return "\n".join(output)


def main() -> None:
    """主函数"""
    import os
    import sys
    
    # 从环境变量获取参数
    query = os.getenv("SEARCH_QUERY")
    if not query:
        logger.error("请设置 SEARCH_QUERY 环境变量")
        sys.exit(1)
        
    k = int(os.getenv("SEARCH_K", "5"))
    collection_name = os.getenv("COLLECTION_NAME")
    no_distances = os.getenv("NO_DISTANCES", "false").lower() == "true"
    no_metadatas = os.getenv("NO_METADATAS", "false").lower() == "true"
    raw_output = os.getenv("RAW_OUTPUT", "false").lower() == "true"
    
    try:
        results = search_documents(
            query=query,
            k=k,
            collection_name=collection_name,
            include_distances=not no_distances,
            include_metadatas=not no_metadatas,
        )
        
        if raw_output:
            import json
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            print(format_search_results(results, query))
            
    except Exception as e:
        logger.error(f"Search failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
