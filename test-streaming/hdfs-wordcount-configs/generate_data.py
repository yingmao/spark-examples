#!/usr/bin/env python3
"""
Enhanced Data Generator - Creates 8 files with realistic timestamps
This supports both batch sizing and windowing demonstrations
"""

import subprocess
import tempfile
import os
from datetime import datetime, timedelta

HDFS_BIN = "/usr/local/hadoop/bin/hdfs"
HDFS_INPUT_DIR = "/streaming-test/input"

# More diverse content with realistic themes
sample_data = [
    # Batch 1: Spark Introduction
    "spark streaming processes data in real time\nspark provides fast distributed computing\ndata processing becomes simple with spark",
    
    # Batch 2: Hadoop Ecosystem  
    "hadoop stores big data reliably across clusters\nhdfs provides distributed storage for hadoop\nhadoop ecosystem includes many useful tools",
    
    # Batch 3: YARN Resource Management
    "yarn manages cluster resources efficiently\nyarn schedules jobs across multiple nodes\nresource management ensures optimal performance",
    
    # Batch 4: Python Integration
    "python makes spark programming accessible\npython integrates seamlessly with spark apis\nprogramming becomes intuitive with python spark",
    
    # Batch 5: Streaming Concepts
    "streaming data arrives continuously in real time\nstreaming applications process infinite data streams\ndata streams require different processing approaches",
    
    # Batch 6: Machine Learning
    "machine learning algorithms process big data efficiently\nspark mllib provides distributed machine learning\nlearning models scale across cluster nodes",
    
    # Batch 7: Performance Optimization
    "performance tuning improves spark job execution\noptimization techniques reduce processing time\ntuning parameters affects cluster utilization significantly",
    
    # Batch 8: Real-world Applications
    "real time analytics enable immediate business decisions\napplications process streaming data for instant insights\nbusiness intelligence requires fast data processing"
]

def upload_to_hdfs(content: str, filename: str) -> None:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        result = subprocess.run(
            [HDFS_BIN, "dfs", "-put", tmp_path, f"{HDFS_INPUT_DIR}/{filename}"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"✓ Uploaded {filename}")
        else:
            print(f"✗ Failed: {filename}")
    finally:
        os.unlink(tmp_path)

def main():
    print("=== Creating 8 sample files for streaming variations ===")
    for i, content in enumerate(sample_data, 1):
        filename = f"batch_{i:02d}.txt"
        upload_to_hdfs(content, filename)
    
    print(f"\n✓ All 8 files uploaded to {HDFS_INPUT_DIR}")
    print("Ready for streaming parameter experiments!")

if __name__ == "__main__":
    main()
