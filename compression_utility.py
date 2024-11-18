#!/usr/bin/env python3
import os
import psycopg2
import gzip
import io
import logging
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import hashlib
from contextlib import contextmanager
from typing import Optional, Dict, Any, Generator, Iterator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import our custom logging configuration
from utils.logging_config import setup_logging


class DatabaseConnectionManager:
    """Manages database connections with context manager and retry logic."""

    def __init__(self, max_retries=3, retry_delay=1):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.db_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }

    @contextmanager
    def get_connection(self) -> Iterator[psycopg2.extensions.connection]:
        """Create a database connection with retry logic."""
        retries = 0
        while True:
            try:
                conn = psycopg2.connect(**self.db_params)
                try:
                    yield conn
                finally:
                    conn.close()
                break
            except psycopg2.Error as e:
                retries += 1
                if retries >= self.max_retries:
                    logging.error(f"Failed to connect to database after {retries} attempts")
                    raise
                logging.warning(f"Database connection attempt {retries} failed: {e}")
                time.sleep(self.retry_delay)


class DataHubAPI:
    """Enhanced DataHub API client with improved error handling and streaming support."""

    def __init__(self):
        self.base_url = os.getenv('DATAHUB_API_URL')
        self.api_key = os.getenv('DATAHUB_API_KEY')
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })
        return session

    def download_file(self, file_id: str) -> Generator[bytes, None, None]:
        """Stream download a file from DataHub."""
        url = f"{self.base_url}/files/{file_id}/download"
        response = self.session.get(url, stream=True)
        response.raise_for_status()

        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                yield chunk

    def upload_file(self, file_data: Iterator[bytes], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Stream upload a file to DataHub."""
        files = {
            'file': ('file.gz', file_data),
            'metadata': (None, json.dumps(metadata))
        }
        response = self.session.post(f"{self.base_url}/files", files=files)
        response.raise_for_status()
        return response.json()

    def verify_file(self, file_id: str, expected_hash: str) -> bool:
        """Verify uploaded file integrity using checksum."""
        hasher = hashlib.sha256()
        for chunk in self.download_file(file_id):
            hasher.update(chunk)
        return hasher.hexdigest() == expected_hash


class FileProcessor:
    """Handles file processing with improved error handling and streaming."""

    def __init__(self, api_client: DataHubAPI, batch_size: int, dry_run: bool):
        self.api_client = api_client
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.stats = ProcessingStatistics()

    def process_file(self, file_metadata: Dict[str, Any]) -> bool:
        """Process a single file with streaming and verification."""
        try:
            # Calculate original file hash while downloading
            hasher = hashlib.sha256()
            compressed_data = io.BytesIO()

            # Stream download and compress
            with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz:
                for chunk in self.api_client.download_file(file_metadata["file_id"]):
                    hasher.update(chunk)
                    gz.write(chunk)

            original_hash = hasher.hexdigest()
            compressed_data_bytes = compressed_data.getvalue()

            if not self.dry_run:
                # Upload compressed file
                metadata = self._prepare_metadata(file_metadata, original_hash)
                new_file = self.api_client.upload_file(
                    iter([compressed_data_bytes]), metadata)

                # Verify upload
                if not self.api_client.verify_file(new_file["file_id"],
                                                   metadata["properties"]["compressed_hash"]):
                    raise ValueError("Upload verification failed")

                # Delete original file only after successful verification
                self.api_client.delete_file(file_metadata["file_id"])

            self.stats.update_success(len(compressed_data_bytes))
            return True

        except Exception as e:
            logging.error(f"Error processing file {file_metadata['file_id']}: {e}")
            self.stats.update_failure()
            return False


class ProcessingStatistics:
    """Tracks processing statistics with enhanced metrics."""

    def __init__(self):
        self.total_files = 0
        self.successful_files = 0
        self.failed_files = 0
        self.total_original_size = 0
        self.total_compressed_size = 0
        self.start_time = datetime.now()

    def update_success(self, compressed_size: int, original_size: int):
        """Update statistics for successful compression."""
        self.successful_files += 1
        self.total_compressed_size += compressed_size
        self.total_original_size += original_size

    def update_failure(self):
        """Update statistics for failed compression."""
        self.failed_files += 1

    def get_summary(self) -> Dict[str, Any]:
        """Generate statistics summary."""
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            "total_files_processed": self.total_files,
            "successful_compressions": self.successful_files,
            "failed_compressions": self.failed_files,
            "total_original_size_mb": self.total_original_size / (1024 * 1024),
            "total_compressed_size_mb": self.total_compressed_size / (1024 * 1024),
            "space_saved_mb": (self.total_original_size - self.total_compressed_size) / (1024 * 1024),
            "compression_ratio": (
                        self.total_compressed_size / self.total_original_size * 100) if self.total_original_size > 0 else 0,
            "processing_time_seconds": duration,
            "average_time_per_file": duration / self.total_files if self.total_files > 0 else 0
        }


def get_files_to_process(db_manager: DatabaseConnectionManager,
                         month: int, year: int,
                         size_threshold: int) -> List[Dict[str, Any]]:
    """Query database for files to process using connection manager."""
    with db_manager.get_connection() as conn:
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

            columns = ['file_id', 'document_id', 'document_type_nm',
                       'size_no', 'uploaded_at_dttm']
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def process_batch(processor: FileProcessor,
                  files: List[Dict[str, Any]],
                  max_threads: int) -> None:
    """Process a batch of files with improved thread management."""
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(processor.process_file, file_metadata): file_metadata["file_id"]
            for file_metadata in files
        }

        with tqdm(total=len(files), unit="file") as pbar:
            for future in as_completed(futures):
                file_id = futures[future]
                try:
                    success = future.result()
                    if success:
                        pbar.set_description(f"Processed {file_id}")
                    else:
                        pbar.set_description(f"Failed {file_id}")
                except Exception as e:
                    logging.error(f"Error processing file {file_id}: {e}")
                    processor.stats.update_failure()
                pbar.update(1)


def main(month: int, year: int, dry_run: bool = False) -> None:
    """Main execution function with improved error handling and monitoring."""
    # Initialize logging
    logger = setup_logging()
    logger.info(f"Starting compression utility - Month: {month}, Year: {year}, Dry Run: {dry_run}")

    try:
        # Initialize components
        db_manager = DatabaseConnectionManager()
        api_client = DataHubAPI()
        batch_size = int(os.getenv('BATCH_SIZE', 100))
        max_threads = int(os.getenv('MAX_THREADS', 4))
        size_threshold = int(os.getenv('SIZE_THRESHOLD', 1024 * 1024))  # 1MB default

        processor = FileProcessor(api_client, batch_size, dry_run)

        # Get files to process
        files = get_files_to_process(db_manager, month, year, size_threshold)
        total_files = len(files)

        if total_files == 0:
            logger.info(f"No files found to process for {year}-{month:02d}")
            return

        logger.info(f"Found {total_files} files to process")

        # Process files in batches
        for i in range(0, total_files, batch_size):
            batch = files[i:i + batch_size]
            process_batch(processor, batch, max_threads)

            # Log intermediate statistics
            if (i + batch_size) % (batch_size * 5) == 0:
                stats = processor.stats.get_summary()
                logger.info(f"Intermediate statistics: {json.dumps(stats, indent=2)}")

        # Log final statistics
        final_stats = processor.stats.get_summary()
        logger.info("Compression completed. Final statistics:")
        logger.info(json.dumps(final_stats, indent=2))

    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compress files using DataHub API.')
    parser.add_argument('month', type=int, help='The month (1-12) to process')
    parser.add_argument('year', type=int, help='The year to process')
    parser.add_argument('--dry-run', action='store_true',
                        help='Perform a dry run without modifying any files')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO', help='Set the logging level')

    args = parser.parse_args()

    if not (1 <= args.month <= 12):
        parser.error("Month must be between 1 and 12")
    if not (2000 <= args.year <= datetime.now().year):
        parser.error(f"Year must be between 2000 and {datetime.now().year}")

    # Set log level if provided via command line
    os.environ['LOG_LEVEL'] = args.log_level

    main(args.month, args.year, args.dry_run)