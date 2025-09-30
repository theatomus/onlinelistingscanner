#!/usr/bin/env python3
"""
Post-processing script to fix missing storage capacity keys in output files.
Reads the final python_parsed_*.txt file and extracts missing storage values
from additional_info when the extractors missed them.
"""
import sys
import re
import argparse
from pathlib import Path

def fix_storage_keys(file_path):
    """Fix missing storage capacity keys in the output file."""
    
    # Read the file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return False
    
    # Look for the additional_info line in title section
    title_section_pattern = r'(====== TITLE DATA ======.*?)(====== METADATA ======)'
    title_match = re.search(title_section_pattern, content, re.DOTALL)
    
    if not title_match:
        print("Could not find TITLE DATA section")
        return False
    
    title_section = title_match.group(1)
    
    # Check if storage capacity keys are missing
    has_storage_cap1 = '[title_storage_capacity1_key]' in title_section
    has_storage_cap2 = '[title_storage_capacity2_key]' in title_section
    
    if has_storage_cap1 and has_storage_cap2:
        print("Storage capacity keys already present, no fix needed")
        return True
    
    # Find the additional_info line
    additional_info_match = re.search(r'\[title_additional_info_key\] additional_info: (.+)', title_section)
    
    if not additional_info_match:
        print("Could not find additional_info in title section")
        return False
    
    additional_info = additional_info_match.group(1).strip()
    print(f"Found additional_info: '{additional_info}'")
    
    # Extract storage capacities from additional_info
    storage_matches = re.findall(r'(\d+)\s*GB', additional_info, re.IGNORECASE)
    print(f"Found storage values: {storage_matches}")
    
    if len(storage_matches) < 2:
        print("Less than 2 storage values found, cannot fix")
        return False
    
    # Create the new storage lines
    storage_cap1 = f"[title_storage_capacity1_key] storage_capacity1: {storage_matches[0]}GB"
    storage_cap2 = f"[title_storage_capacity2_key] storage_capacity2: {storage_matches[1]}GB"
    
    # Clean up additional_info by removing the storage parts
    cleaned_additional_info = re.sub(r'\s*/\s*\d+GB.*', '', additional_info, flags=re.IGNORECASE).strip()
    if cleaned_additional_info.endswith('/'):
        cleaned_additional_info = cleaned_additional_info.rstrip('/')
    
    # Create replacement for additional_info line
    new_additional_info_line = f"[title_additional_info_key] additional_info: {cleaned_additional_info}"
    
    # Insert the storage lines before the additional_info line
    old_additional_info_line = f"[title_additional_info_key] additional_info: {additional_info}"
    
    replacement = f"{storage_cap1}\n{storage_cap2}\n{new_additional_info_line}"
    
    # Replace in the content
    new_content = content.replace(old_additional_info_line, replacement)
    
    # Write the fixed content back
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"✅ SUCCESS: Fixed storage keys in {file_path}")
        print(f"   Added: {storage_cap1}")
        print(f"   Added: {storage_cap2}")
        print(f"   Cleaned additional_info: '{cleaned_additional_info}'")
        return True
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")
        return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Fix missing storage capacity keys in processed files")
    parser.add_argument('file_path', help='Path to the python_parsed_*.txt file to fix')
    
    args = parser.parse_args()
    
    file_path = Path(args.file_path)
    if not file_path.exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    success = fix_storage_keys(file_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
