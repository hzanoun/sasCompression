#!/usr/bin/env python3
"""
Configuration Validation Script
-----------------------------
Validates the configuration file for the DataHub File Compression Utility.
"""

import json
import sys
import psycopg2
import requests
from typing import Dict, Any

def validate_db_connection(db_params: Dict[str, Any]) -> bool:
    """Test database connection with provided parameters."""
    try:
        conn = psycopg2.connect(**db_params)
        conn.close()
        print("✓ Database connection successful")
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

def validate_api_connection(api_url: str, api_key: str) -> bool:
    """Test API connection with provided credentials."""
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(f"{api_url}/health", headers=headers)
        response.raise_for_status()
        print("✓ API connection successful")
        return True
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        return False

def validate_config_file(config_file: str) -> bool:
    """Validate configuration file structure and connections."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        # Check required fields
        required_fields = ['api_base_url', 'api_key', 'db_params']
        for field in required_fields:
            if field not in config:
                print(f"✗ Missing required field: {field}")
                return False

        # Check database parameters
        required_db_params = ['dbname', 'user', 'password', 'host', 'port']
        for param in required_db_params:
            if param not in config['db_params']:
                print(f"✗ Missing database parameter: {param}")
                return False

        # Validate connections
        db_valid = validate_db_connection(config['db_params'])
        api_valid = validate_api_connection(config['api_base_url'], config['api_key'])

        return db_valid and api_valid

    except json.JSONDecodeError:
        print("✗ Invalid JSON format in configuration file")
        return False
    except FileNotFoundError:
        print("✗ Configuration file not found")
        return False
    except Exception as e:
        print(f"✗ Validation error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_config.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    if validate_config_file(config_file):
        print("\n✓ Configuration is valid")
        sys.exit(0)
    else:
        print("\n✗ Configuration validation failed")
        sys.exit(1)