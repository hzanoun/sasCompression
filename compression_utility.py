#!/usr/bin/env python3
"""
DataHub File Compression Utility
-------------------------------
This utility compresses large files stored in DataHub using their REST API.
It implements safe, incremental compression with error handling and rate limiting.

Author: RakBank
Date: 2024-11-13
"""

import requests
import gzip
import io
import logging
import argparse
import json
import psycopg2
import time
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path


# Configure logging
def setup_logging(log_file: str = 'compression_script.log') -> None:
    """Configure logging with both file and console handlers."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


class APIError(Exception):
    """Custom exception for API-related errors."""
    pass


class RateLimiter:
    """Implements exponential backoff for API rate limiting."""

    def __init__(self, base_delay: float = 1, max_delay: float = 60, factor: float = 2):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor
        self.retry_count = 0

    def wait(self) -> None:
        """Calculate and wait for the appropriate delay period."""
        if self.retry_count > 0:
            delay = min(self.base_delay * (self.factor ** (self.retry_count - 1)),
                        self.max_delay)
            time.sleep(delay)
        self.retry_count += 1

    def reset(self) -> None:
        """Reset the retry counter."""
        self.retry_count = 0


class Configuration:
    """Handles loading and validating configuration settings."""

    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> dict:
        """Load configuration from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file {self.config_file} not found")
        except json.JSONDecodeError:
            raise ConfigurationError(f"Invalid JSON in configuration file {self.config_file}")

    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_keys = ['api_base_url', 'api_key', 'db_params']
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise ConfigurationError(f"Missing required configuration keys: {missing_keys}")

    @property
    def api_base_url(self) -> str:
        return self.config['api_base_url']

    @property
    def api_key(self) -> str:
        return self.config['api_key']

    @property
    def db_params(self) -> dict:
        return self.config['db_params']


class DataHubAPI:
    """Handles all interactions with the DataHub REST API."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.rate_limiter = RateLimiter()
        self.session = requests.Session()

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make an API request with rate limiting and retries."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        max_retries = 5

        while True:
            try:
                self.rate_limiter.wait()
                response = self.session.request(method, url, headers=self.headers, **kwargs)
                response.raise_for_status()
                self.rate_limiter.reset()
                return response
            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 429:  # Too Many Requests
                    continue
                if self.rate_limiter.retry_count >= max_retries:
                    raise APIError(f"API request failed after {max_retries} retries: {e}")
                logging.warning(f"Request failed, retrying: {e}")

    def download_file(self, file_id: str) -> bytes:
        """Download a file from DataHub."""
        try:
            response = self._make_request("GET", f"files/{file_id}/download")
            return response.content
        except Exception as e:
            raise APIError(f"Failed to download file {file_id}: {e}")

    def upload_file(self, file_data: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Upload a file to DataHub with metadata."""
        try:
            files = {
                'file': ('file.gz', file_data),
                'metadata': (None, json.dumps(metadata))
            }
            response = self._make_request("POST", "files", files=files)
            return response.json()
        except Exception as e:
            raise APIError(f"Failed to upload file: {e}")

    def delete_file(self, file_id: str) -> None:
        """Delete a file from DataHub."""
        try:
            self._make_request("DELETE", f"files/{file_id}")
        except Exception as e:
            raise APIError(f"Failed to delete file {file_id}: {e}")


class FileProcessor:
    """Handles the processing of individual files and batches."""

    def __init__(self, api_client: DataHubAPI, batch_size: int, dry_run: bool):
        self.api_client = api_client
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.processed_files: Dict[str, bool] = {}
        self.compression_stats = {
            'total_original_size': 0,
            'total_compressed_size': 0,
            'successful_compressions': 0,
            'failed_compressions': 0
        }

    def process_batch(self, files: List[Dict[str, Any]]) -> None:
        """Process a batch of files concurrently."""
        with ThreadPoolExecutor(max_workers=min(self.batch_size, 10)) as executor:
            futures = []
            for file_metadata in files:
                future = executor.submit(self.process_file, file_metadata)
                futures.append((future, file_metadata["file_id"]))

            for future, file_id in futures:
                try:
                    result = future.result()
                    self.processed_files[file_id] = result
                except Exception as e:
                    logging.error(f"Failed to process file {file_id}: {e}")
                    self.processed_files[file_id] = False
                    self.compression_stats['failed_compressions'] += 1

    def process_file(self, file_metadata: Dict[str, Any]) -> bool:
        """Process a single file: download, compress, upload, and verify."""
        try:
            # Download original file
            file_data = self.api_client.download_file(file_metadata["file_id"])
            original_size = len(file_data)
            self.compression_stats['total_original_size'] += original_size

            # Compress the file
            compressed_data = io.BytesIO()
            with gzip.GzipFile(fileobj=compressed_data, mode='wb') as f:
                f.write(file_data)
            compressed_data = compressed_data.getvalue()
            compressed_size = len(compressed_data)
            self.compression_stats['total_compressed_size'] += compressed_size

            if not self.dry_run:
                # Upload compressed file
                metadata = self._prepare_metadata(file_metadata, original_size, compressed_size)
                new_file = self.api_client.upload_file(compressed_data, metadata)

                # Verify upload was successful
                if not self._verify_upload(new_file):
                    raise APIError("Upload verification failed")

                # Delete original file
                self.api_client.delete_file(file_metadata["file_id"])

            logging.info(f"Successfully processed file {file_metadata['file_id']}")
            self.compression_stats['successful_compressions'] += 1
            return True

        except Exception as e:
            logging.error(f"Error processing file {file_metadata['file_id']}: {e}")
            return False

    def _prepare_metadata(self, original_metadata: Dict[str, Any],
                          original_size: int, compressed_size: int) -> Dict[str, Any]:
        """Prepare metadata for the compressed file."""
        return {
            "document_id": original_metadata["document_id"],
            "document_type_nm": f"{original_metadata['document_type_nm']}.gz",
            "properties": {
                "original_upload_date": original_metadata["uploaded_at_dttm"],
                "compressed_from": original_metadata["file_id"],
                "is_compressed": True,
                "compression_date": datetime.now().isoformat(),
                "original_size": original_size,
                "compressed_size": compressed_size,
                "compression_ratio": round(compressed_size / original_size * 100, 2)
            }
        }

    def _verify_upload(self, new_file: Dict[str, Any]) -> bool:
        """Verify that the new file was uploaded successfully."""
        return bool(new_file.get("file_id"))

    def get_statistics(self) -> Dict[str, Any]:
        """Return compression statistics."""
        stats = self.compression_stats.copy()
        if stats['total_original_size'] > 0:
            stats['overall_compression_ratio'] = round(
                stats['total_compressed_size'] / stats['total_original_size'] * 100, 2)
        return stats


def get_files_to_process(conn: psycopg2.extensions.connection,
                         month: int, year: int,
                         size_threshold: int) -> List[Dict[str, Any]]:
    """Query the database for files that need to be processed."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT file_id, document_id, document_type_nm, 
                   size_no, uploaded_at_dttm
            FROM fdhdata.dh_file
            WHERE EXTRACT(MONTH FROM uploaded_at_dttm) = %s
            AND EXTRACT(YEAR FROM uploaded_at_dttm) = %s
            AND size_no > %s
            AND properties->>'is_compressed' IS NULL
            ORDER BY uploaded_at_dttm ASC
        """, (month, year, size_threshold))

        columns = ['file_id', 'document_id', 'document_type_nm', 'size_no', 'uploaded_at_dttm']
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def main(config_file: str, month: int, year: int,
         dry_run: bool = False, batch_size: int = 100,
         size_threshold: int = 1024 * 1024) -> None:
    """Main execution function."""
    setup_logging()
    logging.info(f"Starting compression utility with parameters: "
                 f"month={month}, year={year}, dry_run={dry_run}, "
                 f"batch_size={batch_size}, size_threshold={size_threshold}")

    try:
        # Load configuration
        config = Configuration(config_file)

        # Initialize API client
        api_client = DataHubAPI(config.api_base_url, config.api_key)

        # Initialize file processor
        processor = FileProcessor(api_client, batch_size, dry_run)

        # Connect to database
        conn = psycopg2.connect(**config.db_params)

        # Get files to process
        files_to_process = get_files_to_process(conn, month, year, size_threshold)
        total_files = len(files_to_process)

        if total_files == 0:
            logging.info(f"No files found to process for {year}-{month:02d}")
            return

        logging.info(f"Found {total_files} files to process")

        # Process files in batches
        with tqdm(total=total_files, unit="file") as pbar:
            for i in range(0, total_files, batch_size):
                batch = files_to_process[i:i + batch_size]
                processor.process_batch(batch)
                pbar.update(len(batch))

        # Log statistics
        stats = processor.get_statistics()
        logging.info("Compression Statistics:")
        logging.info(f"Total files processed: {total_files}")
        logging.info(f"Successful compressions: {stats['successful_compressions']}")
        logging.info(f"Failed compressions: {stats['failed_compressions']}")
        logging.info(f"Overall compression ratio: {stats.get('overall_compression_ratio', 0)}%")
        logging.info(f"Total space saved: "
                     f"{(stats['total_original_size'] - stats['total_compressed_size']) / (1024 * 1024):.2f} MB")

        conn.close()

    except Exception as e:
        logging.critical(f"Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compress files using DataHub API.')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('month', type=int, help='The month (1-12) to process')
    parser.add_argument('year', type=int, help='The year to process')
    parser.add_argument('--dry-run', action='store_true',
                        help='Perform a dry run without modifying any files')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of files to process in each batch')
    parser.add_argument('--size-threshold', type=int, default=1024 * 1024,
                        help='Minimum file size in bytes to consider for compression')

    args = parser.parse_args()

    if not (1 <= args.month <= 12):
        parser.error("Month must be between 1 and 12")
    if not (2000 <= args.year <= datetime.now().year):
        parser.error(f"Year must be between 2000 and {datetime.now().year}")

    main(args.config, args.month, args.year, args.dry_run,
         args.batch_size, args.size_threshold)