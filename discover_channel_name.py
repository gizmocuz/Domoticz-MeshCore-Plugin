#!/usr/bin/env python3
"""
MeshCore Channel Name Discovery Script

Brute-forces Dutch city/province names to find matching channel hash bytes.
Uses an external JSON file with Dutch cities (downloaded from SimpleMaps).

MeshCore hash specification:
1. Derive 16-byte key: key = SHA256(name.encode('utf-8')).digest()[0:16]
2. Derive 1-byte hash: hash = SHA256(key).digest()[0]
"""

import hashlib
import json
import sys
from pathlib import Path


def meshcore_hash(name: str) -> int:
    """Compute the MeshCore 1-byte channel hash from a channel name."""
    key = hashlib.sha256(name.encode('utf-8')).digest()[0:16]
    return hashlib.sha256(key).digest()[0]


# Common channel names to test first
COMMON_CHANNELS = [
    (0x03, "#mc-radar"),
    (0x11, "Public"),
    (0xca, "#bot"),
    (0xd9, "#test"),
]


def load_dutch_cities(json_file: str = "nl-cities.json") -> list[str]:
    """Load Dutch city names from a JSON file."""
    cities = []
    
    if not Path(json_file).exists():
        print(f"JSON file '{json_file}' not found.")
        print("Download Dutch cities from: https://simplemaps.com/data/nl-cities")
        print("Click 'Download JSON' and save as 'nl-cities.json'")
        print()
        print("Using built-in fallback list of Dutch cities...")
        cities = load_fallback_cities()
        return cities
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in data:
            if 'city' in item:
                city_name = item['city']
                cities.append(city_name.lower())
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        cities = load_fallback_cities()
    
    return cities


def load_fallback_cities() -> list[str]:
    """Fallback list if JSON not available."""
    provinces = [
        "noord-holland", "zuid-holland", "utrecht", "gelderland",
        "overijssel", "limburg", "noord-brabant", "zeeland",
        "friesland", "drenthe", "groningen", "flevoland",
    ]
    
    cities = [
        "amsterdam", "rotterdam", "utrecht", "eindhoven", "tilburg",
        "groningen", "almere", "breda", "nijmegen", "hilversum",
        "enschede", "arnhem", "haarlem", "amersfoort", "maastricht",
        "leiden", "delft", "dordrecht", "zoetermeer", "zwolle",
        "deventer", "middelburg", "alkmaar", "leeuwarden", "vlaardingen",
        "helmond", "gouda", "venlo", "purmerend", "schiedam",
        "hoorn", "oss", "schagen", "nijkerk", "apeldoorn",
    ]
    
    return provinces + cities


def find_channel_name(target_hash: int, json_file: str = "nl-cities.json") -> list[str]:
    """Find all channel names that match the target hash byte."""
    matches = []
    
    # Step 1: Test common channel names first
    print("Testing common channel names first:")
    print("-" * 60)
    for common_hash, common_name in COMMON_CHANNELS:
        computed = meshcore_hash(common_name)
        status = "✓" if computed == common_hash else "✗"
        print(f"  {status} {common_name}: 0x{computed:02x} (expected: 0x{common_hash:02x})")
        
        if computed == target_hash:
            matches.append(common_name)
    
    if matches:
        return matches
    
    print()
    
    # Step 2: If no match, search Dutch cities
    print("Searching Dutch cities and provinces...")
    names = load_dutch_cities(json_file)
    
    for name in names:
        test_name = f"#{name}"
        hash_byte = meshcore_hash(test_name)
        
        if hash_byte == target_hash:
            matches.append(test_name)
    
    return matches


def main():
    if len(sys.argv) != 2:
        print("Usage: python meshcore_discover.py <hash_byte>")
        print()
        print("Examples:")
        print("  python meshcore_discover.py 0xec")
        print("  python meshcore_discover.py ec")
        print("  python meshcore_discover.py 236")
        print()
        print("Download Dutch cities JSON from:")
        print("  https://simplemaps.com/data/nl-cities")
        print("  Click 'Download JSON' and save as 'nl-cities.json'")
        sys.exit(1)
    
    arg = sys.argv[1]
    
    try:
        if arg.startswith("0x"):
            target_hash = int(arg, 16)
        else:
            try:
                target_hash = int(arg, 16)
            except ValueError:
                target_hash = int(arg)
    except ValueError:
        print(f"Error: Cannot parse '{arg}' as a hex or decimal number")
        sys.exit(1)
    
    if target_hash < 0 or target_hash > 255:
        print(f"Error: Hash byte must be between 0 and 255, got {target_hash}")
        sys.exit(1)
    
    print("=" * 60)
    print(f"MeshCore Channel Name Discovery")
    print(f"Searching for hash: 0x{target_hash:02x} ({target_hash})")
    print("=" * 60)
    print()
    
    matches = find_channel_name(target_hash)
    
    if matches:
        print()
        print(f"Found {len(matches)} match(es):")
        print("-" * 60)
        for match in matches:
            computed = meshcore_hash(match)
            print(f"  ✓ {match} (0x{computed:02x})")
    else:
        print()
        print(f"No matches found for hash 0x{target_hash:02x}")


if __name__ == "__main__":
    main()