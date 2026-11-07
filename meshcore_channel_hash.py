#!/usr/bin/env python3
"""
MeshCore Channel Hash Calculator

Computes the 1-byte channel hash from a channel name using the MeshCore specification:
1. Derive 16-byte key: key = SHA256(name.encode('utf-8')).digest()[0:16]
2. Derive 1-byte hash: hash = SHA256(key).digest()[0]
"""

import hashlib
import sys


def meshcore_hash(name: str) -> int:
    """
    Compute the MeshCore 1-byte channel hash from a channel name.
    
    Args:
        name: The channel name (e.g., "Public", "#test", "#Europe")
    
    Returns:
        The 1-byte hash as an integer (0-255)
    """
    # Step 1: Derive 16-byte key from the name (including '#')
    key = hashlib.sha256(name.encode('utf-8')).digest()[0:16]
    
    # Step 2: Derive 1-byte hash from the 16-byte key array
    hash_byte = hashlib.sha256(key).digest()[0]
    
    return hash_byte


def main():
    if len(sys.argv) != 2:
        print("Usage: python meshcore_hash.py <channel_name>")
        print()
        print("Example:")
        print("  python meshcore_hash.py Public")
        print("  python meshcore_hash.py '#test'")
        print()
        sys.exit(1)
    
    channel_name = sys.argv[1]
    hash_byte = meshcore_hash(channel_name)
    
    # Print in both decimal and hex formats
    print(f"Channel: {channel_name}")
    print(f"Hash byte (hex): 0x{hash_byte:02x}")
    print(f"Hash byte (decimal): {hash_byte}")


if __name__ == "__main__":
    main()