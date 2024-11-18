#!/usr/bin/env python3
import os
import json
import sys
import psycopg2
import requests
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv


class ConfigurationValidator:
    """Validates configuration and environment setup for the compression utility."""

    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self.validation_errors: List[str] = []
        self.validation_warnings: List[str] = []

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse configuration file."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise ValueError(f"Configuration file not found: {self.config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

    def _validate_structure(self) -> bool:
        """Validate configuration file structure."""
        required_sections = ['logging', 'performance', 'api', 'database', 'file_processing']

        for section in required_sections:
            if section not in self.config:
                self.validation_errors.append(f"Missing required section: {section}")

        # Validate specific required fields
        if 'logging' in self.config:
            if 'rotation' not in self.config['logging']:
                self.validation_warnings.append("Logging rotation settings not specified")

        if 'performance' in self.config:
            if 'batch_size' not in self.config['performance']:
                self.validation_errors.append("Batch size not specified in performance settings")

        return len(self.validation_errors) == 0

    def _validate_env_variables(self) -> bool:
        """Validate required environment variables."""
        load_dotenv()

        required_vars = [
            ('DATAHUB_API_URL', 'DataHub API URL'),
            ('DATAHUB_API_KEY', 'DataHub API Key'),
            ('DB_NAME', 'Database name'),
            ('DB_USER', 'Database user'),
            ('DB_PASSWORD', 'Database password'),
            ('DB_HOST', 'Database host'),
            ('DB_PORT', 'Database port')
        ]

        for var, description in required_vars:
            if not os.getenv(var):
                self.validation_errors.append(f"Missing environment variable: {var} ({description})")

        return len(self.validation_errors) == 0

    def _test_database_connection(self) -> bool:
        """Test database connection using environment variables."""
        try:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            conn.close()
            return True
        except Exception as e:
            self.validation_errors.append(f"Database connection failed: {str(e)}")
            return False

    def _test_api_connection(self) -> bool:
        """Test DataHub API connection."""
        try:
            response = requests.get(
                f"{os.getenv('DATAHUB_API_URL')}/health",
                headers={"Authorization": f"Bearer {os.getenv('DATAHUB_API_KEY')}"},
                timeout=10
            )
            response.raise_for_status()
            return True
        except Exception as e:
            self.validation_errors.append(f"API connection failed: {str(e)}")
            return False

    def _validate_permissions(self) -> bool:
        """Validate file permissions and directory access."""
        try:
            log_file = self.config['logging'].get('file', 'compression_script.log')
            log_dir = Path(log_file).parent

            # Check if log directory exists or can be created
            if not log_dir.exists():
                log_dir.mkdir(parents=True, exist_ok=True)

            # Test file creation
            test_file = log_dir / '.test_write'
            test_file.touch()
            test_file.unlink()

            return True
        except Exception as e:
            self.validation_errors.append(f"Permission validation failed: {str(e)}")
            return False

    def validate(self) -> Tuple[bool, List[str], List[str]]:
        """Perform all validation checks."""
        validations = [
            self._validate_structure(),
            self._validate_env_variables(),
            self._test_database_connection(),
            self._test_api_connection(),
            self._validate_permissions()
        ]

        return all(validations), self.validation_errors, self.validation_warnings


def main():
    """Main validation script execution."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if len(sys.argv) != 2:
        logger.error("Usage: python validate_config.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    validator = ConfigurationValidator(config_file)

    try:
        is_valid, errors, warnings = validator.validate()

        if warnings:
            logger.warning("Configuration warnings:")
            for warning in warnings:
                logger.warning(f"  - {warning}")

        if errors:
            logger.error("Configuration errors:")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)

        if is_valid:
            logger.info("Configuration validation successful!")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()