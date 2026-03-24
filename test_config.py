#!/usr/bin/env python3
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the load_config function from MAIN
from MAIN import load_config, create_default_config, CONFIG_PATH

print(f"Testing config auto-creation...")
print(f"Looking for config at: {CONFIG_PATH}")

# Test loading config
config = load_config()
if config:
    print("✅ Config loaded successfully!")
    print(f"Config keys: {list(config.keys())}")
else:
    print("❌ Config loading failed")

# Test if config.json was created
if os.path.exists(CONFIG_PATH):
    print("✅ Config.json file was created automatically!")
    print("File contents:")
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        print(f.read())
else:
    print("❌ Config.json was not created")