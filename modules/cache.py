"""
缓存模块 - LRU + 大小限制的本地文件缓存系统
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
升级点：
  - LRU淘汰策略（而不是满则全清）
  - 访问时间跟踪
  - 更精细的容量管理
  - 统计信息增强
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import pickle
import hashlib
import time
import logging
from pathlib import Path
from typing import Any, Optional, Dict, List, Tuple
from collections import OrderedDict


class LocalFileCache:
    """
    本地文件缓存系统（LRU + 大小限制）
    
    特性：
    - 缓存存储在本地文件中，刷新页面不丢失
    - TTL过期机制
    - LRU淘汰策略（最近最少使用）
    - 容量限制，超出自动淘汰旧缓存
    - 访问时间跟踪
    """
    
    def __init__(
        self, 
        cache_dir: str = ".cache_stock_data", 
        max_size_mb: int = 1500, 
        ttl_seconds: int = 600
    ):
        """
        初始化缓存系统
        
        Args:
            cache_dir: 缓存目录路径
            max_size_mb: 最大缓存大小（MB）
            ttl_seconds: 缓存过期时间（秒）
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl_seconds = ttl_seconds
        
        # LRU跟踪：{cache_key: (access_time, file_size)}
        self.access_tracker: OrderedDict[str, Tuple[float, int]] = OrderedDict()
        
        # 初始化时扫描现有缓存
        self._initialize_tracker()
        
        logging.info(f"缓存系统初始化: {cache_dir}, 最大{max_size_mb}MB, TTL={ttl_seconds}秒")
    
    def _initialize_tracker(self):
        """初始化访问跟踪器（扫描现有缓存文件）"""
        for file in sorted(self.cache_dir.glob("*.pkl"), key=lambda f: f.stat().st_mtime):
            try:
                cache_key = file.stem
                size = file.stat().st_size
                mtime = file.stat().st_mtime
                self.access_tracker[cache_key] = (mtime, size)
            except Exception as e:
                logging.warning(f"初始化跟踪器失败: {file.name}, {e}")
    
    def _get_cache_key(self, key_str: str) -> str:
        """生成缓存文件名（MD5哈希）"""
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """获取缓存文件路径"""
        return self.cache_dir / f"{cache_key}.pkl"
    
    def _get_cache_size(self) -> int:
        """获取缓存目录总大小（字节）"""
        return sum(size for _, size in self.access_tracker.values())
    
    def _evict_lru(self, target_size: int):
        """
        LRU淘汰策略：淘汰最旧的缓存，直到总大小低于目标
        
        Args:
            target_size: 目标大小（字节）
        """
        current_size = self._get_cache_size()
        
        if current_size <= target_size:
            return
        
        logging.info(f"开始LRU淘汰: 当前{current_size/1024/1024:.1f}MB, 目标{target_size/1024/1024:.1f}MB")
        
        evicted_count = 0
        evicted_size = 0
        
        # 按访问时间排序（最旧的在前）
        while current_size > target_size and self.access_tracker:
            # 获取最旧的缓存
            cache_key, (access_time, size) = self.access_tracker.popitem(last=False)
            cache_path = self._get_cache_path(cache_key)
            
            try:
                if cache_path.exists():
                    cache_path.unlink()
                    evicted_count += 1
                    evicted_size += size
                    current_size -= size
                    logging.debug(f"淘汰缓存: {cache_key}, 大小{size/1024:.1f}KB")
            except Exception as e:
                logging.error(f"淘汰失败: {cache_key}, {e}")
        
        logging.info(f"LRU淘汰完成: 删除{evicted_count}个文件, 释放{evicted_size/1024/1024:.1f}MB")
    
    def get(self, key_str: str) -> Optional[Any]:
        """
        获取缓存
        
        Args:
            key_str: 缓存键
            
        Returns:
            缓存数据，如果不存在或过期则返回None
        """
        cache_key = self._get_cache_key(key_str)
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            return None
        
        try:
            # 检查是否过期
            mtime = cache_path.stat().st_mtime
            age = time.time() - mtime
            
            if age > self.ttl_seconds:
                # 过期，删除
                cache_path.unlink()
                if cache_key in self.access_tracker:
                    del self.access_tracker[cache_key]
                logging.debug(f"缓存过期: {key_str[:50]}..., 年龄{age:.1f}秒")
                return None
            
            # 加载数据
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            
            # 更新访问时间（LRU）
            size = cache_path.stat().st_size
            self.access_tracker.move_to_end(cache_key)
            self.access_tracker[cache_key] = (time.time(), size)
            
            logging.info(f"✅ 缓存命中: {key_str[:50]}..., 年龄{age:.1f}秒")
            return data
        
        except (pickle.UnpicklingError, EOFError, ValueError) as e:
            # pickle损坏，删除
            logging.warning(f"缓存文件损坏: {type(e).__name__}, 已删除")
            try:
                cache_path.unlink()
                if cache_key in self.access_tracker:
                    del self.access_tracker[cache_key]
            except:
                pass
            return None
        
        except Exception as e:
            logging.error(f"缓存读取失败: {type(e).__name__}: {str(e)[:100]}")
            try:
                cache_path.unlink()
                if cache_key in self.access_tracker:
                    del self.access_tracker[cache_key]
            except:
                pass
            return None
    
    def set(self, key_str: str, data: Any):
        """
        设置缓存
        
        Args:
            key_str: 缓存键
            data: 要缓存的数据
        """
        cache_key = self._get_cache_key(key_str)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            # 保存缓存
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            
            # 更新跟踪器
            size = cache_path.stat().st_size
            self.access_tracker[cache_key] = (time.time(), size)
            self.access_tracker.move_to_end(cache_key)
            
            # LRU淘汰（保持80%容量）
            current_size = self._get_cache_size()
            if current_size > self.max_size_bytes:
                target_size = int(self.max_size_bytes * 0.8)
                self._evict_lru(target_size)
            
            logging.debug(f"缓存保存: {key_str[:50]}..., 大小{size/1024:.1f}KB")
        
        except Exception as e:
            logging.error(f"缓存保存失败: {type(e).__name__}: {str(e)[:100]}")
    
    def clear_all(self):
        """清空所有缓存"""
        try:
            deleted_count = 0
            for file in self.cache_dir.glob("*.pkl"):
                try:
                    file.unlink()
                    deleted_count += 1
                except:
                    pass
            
            self.access_tracker.clear()
            logging.info(f"缓存已清空: 删除{deleted_count}个文件")
        
        except Exception as e:
            logging.error(f"清空缓存失败: {type(e).__name__}: {str(e)[:100]}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            包含统计信息的字典
        """
        try:
            total_size = self._get_cache_size()
            file_count = len(self.access_tracker)
            usage_pct = (total_size / self.max_size_bytes * 100) if self.max_size_bytes > 0 else 0
            
            # 计算命中率（需要额外跟踪）
            return {
                "total_size_mb": round(total_size / 1024 / 1024, 2),
                "max_size_mb": round(self.max_size_bytes / 1024 / 1024, 2),
                "usage_percent": round(usage_pct, 1),
                "file_count": file_count,
                "ttl_seconds": self.ttl_seconds,
            }
        
        except Exception as e:
            logging.error(f"获取统计信息失败: {e}")
            return {}
    
    def cleanup_expired(self):
        """手动清理过期缓存"""
        current_time = time.time()
        expired_keys = []
        
        for cache_key, (access_time, size) in self.access_tracker.items():
            age = current_time - access_time
            if age > self.ttl_seconds:
                expired_keys.append(cache_key)
        
        for cache_key in expired_keys:
            cache_path = self._get_cache_path(cache_key)
            try:
                if cache_path.exists():
                    cache_path.unlink()
                del self.access_tracker[cache_key]
                logging.debug(f"清理过期缓存: {cache_key}")
            except Exception as e:
                logging.error(f"清理失败: {cache_key}, {e}")
        
        if expired_keys:
            logging.info(f"清理过期缓存: {len(expired_keys)}个文件")


# 全局缓存实例（延迟初始化）
_global_cache: Optional[LocalFileCache] = None


def get_cache(
    cache_dir: str = ".cache_stock_data",
    max_size_mb: int = 1500,
    ttl_seconds: int = 600
) -> LocalFileCache:
    """
    获取全局缓存实例（单例模式）
    
    Args:
        cache_dir: 缓存目录
        max_size_mb: 最大大小（MB）
        ttl_seconds: 过期时间（秒）
        
    Returns:
        缓存实例
    """
    global _global_cache
    
    if _global_cache is None:
        _global_cache = LocalFileCache(cache_dir, max_size_mb, ttl_seconds)
    
    return _global_cache
