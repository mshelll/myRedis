import time
from typing import Dict, Optional, List, Tuple, Any


class Storage:
    """Handles Redis storage operations including cache management and expiry"""
    
    def __init__(self):
        self._cache: Dict[str, Tuple[str, Optional[float]]] = {}
    
    def set(self, key: str, value: str, expiry: Optional[float] = None) -> None:
        """Set a key-value pair with optional expiry"""
        self._cache[key] = (value, expiry)
    
    def get(self, key: str) -> Optional[str]:
        """Get a value for a key, handling expiry"""
        if key not in self._cache:
            return None
        
        value, expiry = self._cache[key]
        
        # Check if key has expired
        if expiry is not None:
            current_time = time.time() * 1000
            # Convert expiry to float if it's a string
            expiry_time = float(expiry) if isinstance(expiry, str) else expiry
            print(f"get expiry expiry={expiry_time} current_time={current_time}")
            if current_time > expiry_time:
                # Remove expired key
                del self._cache[key]
                return None
        
        return value
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching pattern (simple implementation)"""
        if pattern == "*":
            return list(self._cache.keys())
        # For other patterns, return empty list for now
        return []
    
    def exists(self, key: str) -> bool:
        """Check if a key exists"""
        return key in self._cache
    
    def delete(self, key: str) -> bool:
        """Delete a key, returns True if key existed"""
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self) -> None:
        """Clear all keys"""
        self._cache.clear()
    
    def size(self) -> int:
        """Get the number of keys in storage"""
        return len(self._cache)
    
    def get_all(self) -> Dict[str, Tuple[str, Optional[float]]]:
        """Get all key-value pairs (for debugging)"""
        return self._cache.copy()
    
    def load_from_rdb(self, keys_values: List[Tuple[str, str, Optional[float]]]) -> None:
        """Load keys and values from RDB parser"""
        for key, value, expiry in keys_values:
            if key:  # Skip empty keys
                self._cache[key] = (value, expiry) 