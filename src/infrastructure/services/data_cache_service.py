# src/infrastructure/services/data_cache_service.py
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import hashlib
import pickle

class DataCacheService:
    """Service for caching financial data to reduce API calls."""
    
    def __init__(self, cache_dir: str = None, ttl_minutes: int = 60):
        self.logger = logging.getLogger(__name__)
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), '.cache')
        self.ttl = timedelta(minutes=ttl_minutes)
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def get(self, cache_key: str) -> Optional[Any]:
        try:
            file_path = self._get_cache_path(cache_key)
            if not os.path.exists(file_path):
                return None
            
            with open(file_path, 'rb') as f:
                cache_data = pickle.load(f)
            
            timestamp = cache_data.get('timestamp')
            if timestamp:
                cache_datetime = datetime.fromisoformat(timestamp)
                if datetime.now() - cache_datetime > self.ttl:
                    self.logger.debug(f"Cache expired for key: {cache_key}")
                    return None
            
            return cache_data.get('data')
            
        except Exception as e:
            self.logger.warning(f"Error retrieving from cache: {str(e)}")
            return None
    
    def set(self, cache_key: str, data: Any) -> bool:
        try:
            file_path = self._get_cache_path(cache_key)
            
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            with open(file_path, 'wb') as f:
                pickle.dump(cache_data, f)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"Error saving to cache: {str(e)}")
            return False
    
    def invalidate(self, cache_key: str) -> bool:
        try:
            file_path = self._get_cache_path(cache_key)
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Error invalidating cache: {str(e)}")
            return False
    
    def clear(self) -> bool:
        try:
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return True
        except Exception as e:
            self.logger.warning(f"Error clearing cache: {str(e)}")
            return False
    
    def _get_cache_path(self, cache_key: str) -> str:
        hashed_key = hashlib.md5(cache_key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hashed_key}.cache")