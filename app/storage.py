import time
import threading
from typing import Dict, Optional, List, Tuple
from collections import defaultdict, deque


class Storage:
    """Handles Redis storage operations including cache management and expiry"""
    
    def __init__(self):
        self._cache: Dict[str, Tuple[str, Optional[float]]] = {}
        # Add blocking operations support
        self._waiting_clients: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()
    
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
            print(f"get expiry expiry={expiry_time} "
                  f"current_time={current_time}")
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
    
    def load_from_rdb(self, keys_values: List[Tuple[str, str, 
                      Optional[float]]]) -> None:
        """Load keys and values from RDB parser"""
        for key, value, expiry in keys_values:
            if key:  # Skip empty keys
                self._cache[key] = (value, expiry)

    def rpush(self, key: str, *values: str) -> int:
        """Append one or multiple values to a list, create list if not 
        exists. Returns new length."""
        with self._lock:
            # Check if key exists and is a list
            if key in self._cache:
                current_value, expiry = self._cache[key]
                # If not a list, convert to list
                if not isinstance(current_value, list):
                    current_value = [current_value]
            else:
                current_value = []
                expiry = None
            current_value.extend(values)
            self._cache[key] = (current_value, expiry)
            
            # Notify waiting clients
            self._notify_waiting_clients(key)
            
            return len(current_value)
    
    def lpush(self, key: str, *values: str) -> int:
        """Prepend one or more values to a list, create list if not 
        exists. Returns new length."""
        with self._lock:
            if key in self._cache:
                current_value, expiry = self._cache[key]
                if not isinstance(current_value, list):
                    current_value = [current_value]
            else:
                current_value = []
                expiry = None
            current_value = list(reversed(values)) + current_value
            self._cache[key] = (current_value, expiry)
            
            # Notify waiting clients
            self._notify_waiting_clients(key)
            
            return len(current_value)

    def _notify_waiting_clients(self, key: str):
        """Notify waiting clients that a key now has data"""
        if key in self._waiting_clients and self._waiting_clients[key]:
            # Only notify the first waiting client (FIFO)
            event = self._waiting_clients[key].popleft()
            event.set()

    def lrange(self, key: str, start: int, stop: int) -> list:
        """Get a range of elements from a list."""
        if key not in self._cache:
            return []
        value, expiry = self._cache[key]
        if not isinstance(value, list):
            return []
        # Handle negative indices like Redis
        list_len = len(value)
        if start < 0:
            start = max(0, list_len + start)
        if stop < 0:
            stop = max(0, list_len + stop)
        print(f"lrange key={key} start={start} stop={stop}")
        # Redis LRANGE is inclusive of stop
        stop = min(stop, list_len - 1)
        if start > stop or start >= list_len:
            return []
        return value[start:stop+1]
    
    def llen(self, key: str) -> int:
        """Get the length of a list."""
        if key not in self._cache:
            return 0
        value, expiry = self._cache[key]
        if not isinstance(value, list):
            return 0
        return len(value)
    
    def lpop(self, key: str, pop_count: int) -> Optional[str]:
        """Remove and return the first element of a list."""
        if key not in self._cache:
            return None
        value, expiry = self._cache[key]
        if not isinstance(value, list):
            return None
        elems = []
        for i in range(pop_count):
            elems += [value.pop(0)]
        return elems
    def blpop(self, key: str, timeout: float) -> Optional[List[str]]:
        """Remove and return the first element of a list, blocking if empty."""
        # First check if the key already has data
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if isinstance(value, list) and len(value) > 0:
                    element = value.pop(0)
                    # If list becomes empty, remove the key
                    if not value:
                        del self._cache[key]
                    else:
                        self._cache[key] = (value, expiry)
                    return [key, element]
    
        # Key doesn't exist or is empty, so we need to wait
        if timeout == 0:
            # Block indefinitely
            event = threading.Event()
            with self._lock:
                self._waiting_clients[key].append(event)
        
            # Wait for the event to be set
            event.wait()
        
            # Try to get the element
            with self._lock:
                if key in self._cache:
                    value, expiry = self._cache[key]
                    if isinstance(value, list) and len(value) > 0:
                        element = value.pop(0)
                        # If list becomes empty, remove the key
                        if not value:
                            del self._cache[key]
                        else:
                            self._cache[key] = (value, expiry)
                        return [key, element]
        else:
            # Block with timeout (timeout is in seconds as float)
            event = threading.Event()
            with self._lock:
                self._waiting_clients[key].append(event)
        
            # Wait for the event with timeout
            if event.wait(timeout):
                # Try to get the element
                with self._lock:
                    if key in self._cache:
                        value, expiry = self._cache[key]
                        if isinstance(value, list) and len(value) > 0:
                            element = value.pop(0)
                            # If list becomes empty, remove the key
                            if not value:
                                del self._cache[key]
                            else:
                                self._cache[key] = (value, expiry)
                            return [key, element]
            else:
                # Timeout occurred, remove from waiting list
                with self._lock:
                    try:
                        self._waiting_clients[key].remove(event)
                    except ValueError:
                        pass  # Event might have been removed already
    
        return None
