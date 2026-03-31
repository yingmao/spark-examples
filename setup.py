#!/usr/bin/env python3
import argparse
import ipaddress
import os
import subprocess
from pathlib import Path
from typing import List

# Version configuration
SPARK_VER = "4.1.1"
HADOOP_TAG = "3"
BASE_DIR = Path("/spark-examples")

MANAGER_FILE = BASE_DIR / "manager"
WORKERS_FILE = BASE_DIR / "workers"
SPARK_TGZ_NAME = f"spark-{SPARK_VER}-bin-hadoop{HADOOP_TAG}.tgz"
SPARK_TGZ = BASE_DIR / SPARK_TGZ_NAME

# Installation paths
SPARK_INSTALL_DIR = Path(f"/usr/local/spark-{SPARK_VER}-bin-hadoop{HADOOP_TAG}")
SPARK_SYMLINK = Path("/usr/local/spark")
SPARK_CONF_DIR = SPARK_SYMLINK / "conf"

# System paths (matching your existing Hadoop setup)
JAVA_HOME = "/usr/lib/jvm/java-21-openjdk-amd64"
HADOOP_HOME = "/usr/local/hadoop"
HADOOP_CONF_DIR = f"{HADOOP_HOME}/etc/hadoop"
PYTHON_BIN = "/usr/bin/python3"


def sh(cmd: str, check: bool = True) -> None:
    """Execute shell command with logging."""
    print(f"[CMD] {cmd}")
    subprocess.run(cmd, shell=True, check=check)


def write_file(path: Path, content: str) -> None:
    """Write content to file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"Finished Config: {path}")


def read_manager_ip() -> str:
    """Read and validate manager IP address."""
    mip = MANAGER_FILE.read_text(encoding="utf-8", errors="replace").strip()
    ipaddress.ip_address(mip)  # Validate IP format
    return mip


def read_worker_ips() -> List[str]:
    """Read and validate worker IP addresses, properly handling '-' symbols."""
    text = WORKERS_FILE.read_bytes().decode("utf-8", errors="replace")
    ips: List[str] = []

    for line in text.splitlines():
        # Clean line: remove BOM, NBSP, CRLF, trim whitespace
        s = line.replace("\ufeff", "").replace("\u00a0", " ").replace("\r", "").strip()

        # Skip empty lines, '-' symbols, and comments
        if not s or s == "-" or s.startswith("#"):
            continue

        try:
            ipaddress.ip_address(s)  # Validate IP format
            ips.append(s)
        except ValueError:
            print(f"[WARN] Ignoring non-IP line in workers: {s!r}")

    return ips


def ensure_root() -> None:
    """Ensure script is running as root."""
    if os.geteuid() != 0:
        raise SystemExit("Run as root (lab): ssh root@node ...")


def install_dependencies() -> None:
    """Install required dependencies for data processing."""
    sh("apt-get update -y")
    sh("apt-get install -y python3 python3-pip python-is-python3 curl", check=False)

    # Install only data science libraries, skip pyspark
    sh("pip3 install --break-system-packages numpy pandas", check=False)



def install_spark() -> None:
    """Download, extract, and install Spark."""
    # Clean previous installation
    sh(f"rm -rf {SPARK_INSTALL_DIR} || true", check=False)
    sh(f"rm -f {SPARK_SYMLINK} || true", check=False)

    # Download Spark if not present
    if not SPARK_TGZ.exists():
        url = f"https://archive.apache.org/dist/spark/spark-{SPARK_VER}/{SPARK_TGZ_NAME}"
        print(f"Downloading Spark {SPARK_VER} from {url}")
        sh(f"curl -L -o {SPARK_TGZ} {url}")

    # Extract and create symlink
    sh(f"tar -xzf {SPARK_TGZ} -C /usr/local/")
    sh(f"ln -s {SPARK_INSTALL_DIR} {SPARK_SYMLINK}")


def configure_spark_env(mip: str, workers: List[str]) -> None:
    """Configure Spark environment files."""

    # Create workers file (modern replacement for slaves)
    write_file(SPARK_CONF_DIR / "workers", "\n".join(workers) + ("\n" if workers else ""))

    # Configure spark-env.sh with PySpark and Streaming support
    spark_env_content = f"""#!/usr/bin/env bash

# Java configuration
export JAVA_HOME={JAVA_HOME}

# Hadoop integration
export HADOOP_HOME={HADOOP_HOME}
export HADOOP_CONF_DIR={HADOOP_CONF_DIR}

# Spark cluster configuration
export SPARK_MASTER_HOST={mip}
export SPARK_WORKER_MEMORY=1g
export SPARK_WORKER_CORES=2

# Python configuration for PySpark (critical for cluster consistency)
export PYSPARK_PYTHON={PYTHON_BIN}
export PYSPARK_DRIVER_PYTHON={PYTHON_BIN}

# Hadoop classpath for YARN integration
export SPARK_DIST_CLASSPATH=$({HADOOP_HOME}/bin/hadoop classpath)
"""

    write_file(SPARK_CONF_DIR / "spark-env.sh", spark_env_content)
    sh(f"chmod +x {SPARK_CONF_DIR}/spark-env.sh")


def configure_spark_defaults(mip: str) -> None:
    """Configure Spark defaults for YARN and Streaming."""

    # HDFS directory for Spark event logs and streaming checkpoints
    eventlog_dir = f"hdfs://{mip}:9000/spark-history"

    spark_defaults_content = f"""# Spark on YARN Configuration
spark.master                      yarn
spark.submit.deployMode           client

# Application settings
spark.app.name                    SparkApp
spark.ui.port                     4040

# Event logging and history (important for Streaming monitoring)
spark.eventLog.enabled            true
spark.eventLog.dir                {eventlog_dir}
spark.history.fs.logDirectory     {eventlog_dir}
spark.history.ui.port             18080

# Resource allocation (adjust for your cluster)
spark.executor.memory             1g
spark.executor.cores              1
spark.driver.memory               1g
spark.executor.memoryOverhead     512

# YARN integration
spark.hadoop.yarn.resourcemanager.hostname {mip}

# Serialization (recommended for performance, especially streaming)
spark.serializer                  org.apache.spark.serializer.KryoSerializer

# Streaming-specific configurations
spark.streaming.stopGracefullyOnShutdown true
spark.sql.streaming.checkpointLocation hdfs://{mip}:9000/spark-checkpoints
"""

    write_file(SPARK_CONF_DIR / "spark-defaults.conf", spark_defaults_content)



def update_system_environment() -> None:
    """Update system environment for convenient Spark/PySpark usage."""
    bashrc = Path("/root/.bashrc")

    # Read existing bashrc
    lines = []
    if bashrc.exists():
        lines = bashrc.read_text().splitlines()

    def ensure_export_line(prefix: str, line: str) -> None:
        nonlocal lines
        # Remove old lines with same prefix
        lines[:] = [l for l in lines if not l.strip().startswith(prefix)]
        lines.append(line)

    # Add/update environment variables
    ensure_export_line("export JAVA_HOME=", f"export JAVA_HOME={JAVA_HOME}")
    ensure_export_line("export HADOOP_HOME=", f"export HADOOP_HOME={HADOOP_HOME}")
    ensure_export_line("export HADOOP_CONF_DIR=", f"export HADOOP_CONF_DIR={HADOOP_CONF_DIR}")
    ensure_export_line("export SPARK_HOME=", f"export SPARK_HOME={SPARK_SYMLINK}")
    ensure_export_line("export PYSPARK_PYTHON=", f"export PYSPARK_PYTHON={PYTHON_BIN}")
    ensure_export_line("export PYSPARK_DRIVER_PYTHON=", f"export PYSPARK_DRIVER_PYTHON={PYTHON_BIN}")
    ensure_export_line(
        'export PATH="$PATH:/usr/local',
        'export PATH="$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin"'
    )

    bashrc.write_text("\n".join(lines) + "\n")
    print("Updated /root/.bashrc with Spark/PySpark environment variables.")


def verify_installation() -> None:
    """Verify Spark and PySpark installation."""
    required_files = [
        SPARK_SYMLINK / "bin/pyspark",
        SPARK_SYMLINK / "bin/spark-submit",
        SPARK_SYMLINK / "bin/spark-shell",
        SPARK_CONF_DIR / "spark-env.sh",
        SPARK_CONF_DIR / "spark-defaults.conf"
    ]

    for file_path in required_files:
        if not file_path.exists():
            raise SystemExit(f"[FATAL] Missing required file: {file_path}")

    print("✓ Spark and PySpark installation verification passed")


def create_hdfs_directories_if_manager(role: str, mip: str) -> None:
    """Create HDFS directories for Spark history and checkpoints (manager only)."""
    if role != "manager":
        print("Worker node: skip HDFS directory creation.")
        return

    if not Path(HADOOP_HOME).exists():
        print("[WARN] HADOOP_HOME not found. Skipping HDFS directory setup.")
        return

    print("Creating HDFS directories for Spark...")
    # Event logs directory
    sh(f"{HADOOP_HOME}/bin/hdfs dfs -mkdir -p /spark-history", check=False)
    sh(f"{HADOOP_HOME}/bin/hdfs dfs -chmod 777 /spark-history", check=False)

    # Checkpoints directory for Streaming applications
    sh(f"{HADOOP_HOME}/bin/hdfs dfs -mkdir -p /spark-checkpoints", check=False)
    sh(f"{HADOOP_HOME}/bin/hdfs dfs -chmod 777 /spark-checkpoints", check=False)


def configure_yarn_site(mip: str) -> None:
    """Generate YARN configuration with FIFO scheduler for single-user lab environment."""
    yarn_site_path = Path(f"{HADOOP_CONF_DIR}/yarn-site.xml")

    yarn_site_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <!-- ResourceManager configuration (fixes 0.0.0.0 issue) -->
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>{mip}</value>
  </property>

  <!-- Use FIFO scheduler for simple single-user lab cluster -->
  <property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler</value>
  </property>

  <!-- Shuffle service for Spark/MapReduce -->
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>

  <!-- Resource allocation (adjust based on your VM specs) -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>3072</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>2</value>
  </property>

  <!-- Disable strict memory checks for lab environment -->
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>

  <!-- Web UI binding -->
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>0.0.0.0:8088</value>
  </property>

  <!-- MapReduce JobHistory endpoints -->
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>{mip}:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>0.0.0.0:19888</value>
  </property>
</configuration>
"""

    write_file(yarn_site_path, yarn_site_content)
    print("✓ Configured YARN with FIFO scheduler for single-user lab environment")



def main() -> None:
    """Main installation workflow."""
    ap = argparse.ArgumentParser(
        description="Install and configure Apache Spark 4.1.1 for Python applications and Streaming"
    )
    ap.add_argument("--role", required=True, choices=["manager", "worker"],
                   help="Node role in the cluster")
    args = ap.parse_args()

    ensure_root()

    # Read cluster configuration
    mip = read_manager_ip()
    workers = read_worker_ips()

    print(f"Role: {args.role}")
    print(f"Manager IP: {mip}")
    print(f"Workers: {workers}")
    print(f"Installing Spark {SPARK_VER} with PySpark and Streaming support...")

    # Installation workflow
    install_dependencies()
    install_spark()
    configure_spark_env(mip, workers)
    configure_spark_defaults(mip)
    update_system_environment()
    verify_installation()
    create_hdfs_directories_if_manager(args.role, mip)
    # ADD THIS LINE: Automatically fix YARN configuration
    configure_yarn_site(mip)

    print(f"✓ Successfully configured Spark {SPARK_VER} on {args.role} node for Python applications.")


if __name__ == "__main__":
    main()
