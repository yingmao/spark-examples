#!/usr/bin/env python3
"""
Simple data generator for HDFS Streaming demo.
Creates 5 small text files and uploads them to HDFS.
"""

import subprocess

HDFS_BIN = "/usr/local/hadoop/bin/hdfs"
HDFS_INPUT_DIR = "/streaming-test/input"

# Simple sample data for each file
sample_data = [
    "spark streaming processes data in real time\nspark is fast and powerful\ndata processing made easy",
    "hadoop stores big data reliably\nhdfs provides distributed storage\nhadoop ecosystem is comprehensive", 
    "yarn manages cluster resources efficiently\nyarn schedules jobs across nodes\nresource management simplified",
    "python makes spark programming simple\npython integrates well with spark\nprogramming becomes more accessible",
    "students learn streaming concepts today\nstreaming data processing explained\nconcepts become clear through practice"
]

def upload_to_hdfs(content: str, filename: str) -> None:
    """Write content to a local temp file, then upload to HDFS."""
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        result = subprocess.run(
            [HDFS_BIN, "dfs", "-put", tmp_path, f"{HDFS_INPUT_DIR}/{filename}"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"✓ Uploaded {filename} to HDFS")
        else:
            print(f"✗ Failed to upload {filename}: {result.stderr}")
    finally:
        os.unlink(tmp_path)

def main():
    print("=== Creating 5 sample files for streaming demo ===")
    
    for i, content in enumerate(sample_data, 1):
        filename = f"batch_{i:02d}.txt"
        upload_to_hdfs(content, filename)
    
    print(f"\n✓ All files uploaded to {HDFS_INPUT_DIR}")
    print("Ready for streaming demo!")

if __name__ == "__main__":
    main()
