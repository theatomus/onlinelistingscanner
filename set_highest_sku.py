#!/usr/bin/env python3
"""
Utility script to manually set the highest SKU number for tracking.
This is useful when you want to reset the baseline for counting new listings.
"""

import sys
import os

# Prefer state-based path used by scan_monitor; fall back to repo root file
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, 'state')
HIGHEST_SKU_FILE = os.path.join(STATE_DIR, 'highest_sku_number.txt')
HIGHEST_SKU_FILE_FALLBACK = os.path.join(BASE_DIR, 'highest_sku_number.txt')

def get_current_highest_sku():
    """Get the current highest SKU number"""
    try:
        # Primary: state file
        if os.path.exists(HIGHEST_SKU_FILE):
            with open(HIGHEST_SKU_FILE, 'r') as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
        # Fallback: legacy root file
        if os.path.exists(HIGHEST_SKU_FILE_FALLBACK):
            with open(HIGHEST_SKU_FILE_FALLBACK, 'r') as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
    except Exception as e:
        print(f"Error reading highest SKU file: {e}")
    return 0

def set_highest_sku(new_sku):
    """Set a new highest SKU number"""
    try:
        # Ensure directory exists
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
        except Exception:
            pass
        with open(HIGHEST_SKU_FILE, 'w') as f:
            f.write(str(new_sku))
        print(f"✅ Successfully set highest SKU to: {new_sku}")
        return True
    except Exception as e:
        print(f"❌ Error setting highest SKU: {e}")
        return False

def main():
    current_sku = get_current_highest_sku()
    print(f"📊 Current highest SKU: {current_sku}")
    
    if len(sys.argv) != 2:
        print("\nUsage: python set_highest_sku.py <number>")
        print("Example: python set_highest_sku.py 2600")
        print("\nThis will set the baseline so only listings with SKU >= 2600 are counted as 'new'")
        print("(Note: SKUs >= 20000 are automatically excluded)")
        sys.exit(1)
    
    try:
        new_sku = int(sys.argv[1])
        
        if new_sku < 0:
            print("❌ Error: SKU number must be >= 0")
            sys.exit(1)
        
        if new_sku >= 20000:
            print("⚠️ Warning: SKU >= 20000 will be excluded from counting anyway")
            print("   Are you sure you want to set it this high? (y/n): ", end='')
            response = input().lower()
            if response != 'y' and response != 'yes':
                print("Operation cancelled.")
                sys.exit(1)
        
        if new_sku < current_sku:
            print(f"⚠️ Warning: New SKU ({new_sku}) is lower than current ({current_sku})")
            print("   This will cause more listings to be counted as 'new'. Continue? (y/n): ", end='')
            response = input().lower()
            if response != 'y' and response != 'yes':
                print("Operation cancelled.")
                sys.exit(1)
        
        if set_highest_sku(new_sku):
            print(f"📈 From now on, only listings with SKU >= {new_sku} (and < 20000) will be counted")
        
    except ValueError:
        print(f"❌ Error: '{sys.argv[1]}' is not a valid number")
        sys.exit(1)

if __name__ == "__main__":
    main() 