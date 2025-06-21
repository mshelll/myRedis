import os
from typing import List, Tuple, Optional


class RDBParser:
    """Handles parsing of Redis Database (RDB) files"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def load_keys_from_rdb(self) -> List[Tuple[str, str, Optional[str]]]:
        """Load keys and values from RDB file
        
        Returns:
            list: List of (key, value, expiry) tuples
        """
        try:
            if not os.path.exists(self.db_path):
                print(f"RDB file does not exist: {self.db_path}")
                return []
            
            keys_values = self.read_keys_from_rdb()
            if keys_values:
                print(f"Loaded {len(keys_values)} keys from RDB file")
            return keys_values
        except Exception as e:
            print(f"Error loading cache from RDB: {e}")
            return []

    def read_keys_from_rdb(self) -> List[Tuple[str, str, Optional[str]]]:
        """Read keys and values from RDB file
    
        Returns:
            list: List of (key, value, expiry) tuples
        """
        try:
            with open(self.db_path, 'rb') as fd:
                content = fd.read()
            
            # Find DB section (between FB and FF markers)
            start_marker = b'\xfb'
            end_marker = b'\xff'
            
            start_idx = content.find(start_marker)
            end_idx = content.find(end_marker, start_idx) if start_idx != -1 else -1
            
            if start_idx == -1 or end_idx == -1:
                print("Could not find database section in RDB file")
                return []
            
            # Extract DB section (skip FB marker but include everything up to FF marker)
            db_section = content[start_idx+1:end_idx]
            
            # First byte represents the number of keys
            num_keys = db_section[0]
            print(f"RDB header indicates {num_keys} keys")
            
            print(f"DB section length: {len(db_section)} bytes")
            print(f"DB section hex: {db_section.hex()}")
            print(f"DB section : {db_section}")
            
            # Parse key-value pairs
            keys_values = []
            i = 2  # Start after the key count byte
            
            while i < len(db_section):
                try:
                    print(f'{db_section[i]=}')
                    # Check for the expiry marker (0xFC)

                    if i < len(db_section) and db_section[i] == 0xFC:
                        print(f"Current byte: 0x{db_section[i]:x}")
                        i += 1  # Skip the FC marker
                        
                        # Parse expiry timestamp (milliseconds since epoch)
                        # Extract a 4-byte timestamp from the next 8 bytes
                        expiry_bytes = db_section[i:i+8]
                        # Use bytes 1-4 (little endian) which seems to contain the actual timestamp
                        # The byte at index 0 is likely a type flag
                        expiry_ms = int.from_bytes(expiry_bytes, byteorder='little')
                        
                        # Convert to seconds for easier comparison with current time
                        # Redis stores expiry as milliseconds since epoch
                        expiry = str(expiry_ms)
                        i += 8
                        
                        print(f"expiry={expiry}")
                    # else:
                    #     # No expiry
                    #     expiry = 0
                    
                    # Get key length
                    if i >= len(db_section):
                        break
                    key_len = db_section[i]
                    i += 1
                    
                    # Skip zero-length keys
                    if key_len == 0:
                        continue
                    
                    # Extract key
                    if i + key_len > len(db_section):
                        break
                    key = db_section[i:i+key_len].decode('utf-8', errors='replace')
                    i += key_len
                    
                    # Get value length
                    if i >= len(db_section):
                        break
                    val_len = db_section[i]
                    i += 1
                    
                    # Extract value
                    if i + val_len > len(db_section):
                        break
                    value = db_section[i:i+val_len].decode('utf-8', errors='replace')
                    i += val_len
                    
                    if 'expiry' not in locals():
                        expiry = None
                    print(f"Extracted key='{key}', value='{value}' expiry={expiry}")
                    keys_values.append((key, value, expiry))
                    
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    print(f"Error parsing key-value pair at position {i}: {e}")
                    i += 1  # Skip this byte and continue
            
            print(f"Successfully extracted {len(keys_values)} key-value pairs: {keys_values}")
            return keys_values
            
        except Exception as e:
            print(f"Error reading RDB file: {e}")
            import traceback
            traceback.print_exc()
            return [] 