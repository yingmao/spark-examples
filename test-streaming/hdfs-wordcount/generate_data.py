#!/usr/bin/env python3
"""
Random text data generator for HDFS Streaming test.
Continuously generates text files and uploads them to HDFS.
"""

import random
import time
import subprocess
import tempfile
import os
from datetime import datetime

HDFS_INPUT_DIR = "hdfs:///streaming-test/input"
HDFS_BIN = "/usr/local/hadoop/bin/hdfs"

# Sample vocabulary for generating realistic text
TECH_WORDS = [
    "spark", "hadoop", "yarn", "hdfs", "streaming", "python", "java", "scala",
    "cluster", "node", "data", "bigdata", "analytics", "processing", "distributed",
    "mapreduce", "dataframe", "rdd", "sql", "machine", "learning", "cloud"
]

COMMON_WORDS = [
    "the", "and", "for", "with", "from", "this", "that", "will", "have",
    "been", "can", "more", "new", "system", "work", "time", "way", "use"
]

ALL_WORDS = TECH_WORDS + COMMON_WORDS


def hdfs_put(local_path: str, hdfs_path: str) -> bool:
    """Upload a local file to HDFS. Returns True if successful."""
    result = subprocess.run(
        [HDFS_BIN, "dfs", "-put", local_path, hdfs_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[ERROR] HDFS upload failed: {result.stderr.strip()}")
        return False
    return True


def generate_text_content(num_lines: int = 15) -> str:
    """Generate random text content with realistic word distribution."""
    lines = []
    for _ in range(num_lines):
        # Create lines with 8-12 words each
        line_length = random.randint(8, 12)
        words = []
        for _ in range(line_length):
            # 70% chance of tech words, 30% common words for realistic distribution
            word_list = TECH_WORDS if random.random() < 0.7 else COMMON_WORDS
            words.append(random.choice(word_list))
        lines.append(" ".join(words))
    return "\n".join(lines)


def main() -> None:
    batch_num = 0
    interval = 5  # seconds between file uploads

    print("=== HDFS Streaming Data Generator Started ===")
    print(f"Target directory: {HDFS_INPUT_DIR}")
    print(f"Upload interval:  {interval} seconds")
    print("Press Ctrl+C to stop.")
    print("=" * 50)

    try:
        while True:
            batch_num += 1
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"stream_batch_{timestamp}_{batch_num:04d}.txt"
            hdfs_path = f"{HDFS_INPUT_DIR}/{filename}"

            # Generate content and write to temporary file
            content = generate_text_content(num_lines=15)
            
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name

            try:
                if hdfs_put(tmp_path, hdfs_path):
                    word_count = len(content.split())
                    print(f"[Batch {batch_num:04d}] Uploaded: {filename} ({word_count} words)")
                else:
                    print(f"[Batch {batch_num:04d}] FAILED: {filename}")
            finally:
                os.unlink(tmp_path)  # Always clean up temp file

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n=== Data Generator Stopped (Generated {batch_num} batches) ===")


if __name__ == "__main__":
    main()
