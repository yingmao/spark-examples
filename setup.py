import os
import sys, socket


def writeHadoopConfigFile(name,xml):
    f = open("/usr/local/hadoop/etc/hadoop/" + name,"w")
    f.write(xml)
    f.close()

def writeSparkConfigFile(name,xml):
    f = open("/usr/local/spark/conf/" + name,"w")
    f.write(xml)
    f.close()



mf = open("manager","r")
sf = open("workers","r")
mip = mf.read().strip()
sip = sf.read().replace("-","")
mf.close()
sf.close()

# os.system("apt-get install -y python3-pip && curl -fsSL -o- https://bootstrap.pypa.io/pip/3.5/get-pip.py | python3.5 && hash -r && pip install --upgrade pip && pip install pyhdfs")

os.system("sudo apt install software-properties-common -y &&  sudo add-apt-repository ppa:deadsnakes/ppa -y && sudo apt update && sudo apt install python3.9 -y")
os.system("apt-get update -y && apt-get install python -y && apt-get install -y default-jdk && apt-get install -y curl && apt-get install -y maven && sudo apt-get -y install python3-pip && pip3 install pyhdfs && pip3 install numpy pyspark")

#clear first
os.system("rm -rf /usr/local/hadoop-3.3.1/ && unlink /usr/local/hadoop && rm -rf /data/hadoop/ && rm -rf /usr/local/spark-3.2.1-bin-hadoop3.2/ && unlink /usr/local/spark && rm -rf /usr/local/scala-2.11.12 && unlink /usr/local/scala")
os.system("sed -i /JAVA_HOME/d /root/.bashrc && sed -i /hadoop/d /root/.bashrc && sed -i /StrictHostKeyChecking/d /etc/ssh/ssh_config")

if os.path.exists("/hdfs-test/hadoop-3.3.1.tar.gz"):
	os.system("cp /hdfs-test/hadoop-3.3.1.tar.gz /spark-examples/")

if not os.path.exists("/hdfs-test/hadoop-3.3.1.tar.gz"):
	print("Downloading Hadoop 3.3.1....")
	os.system("curl https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz > /spark-examples/hadoop-3.3.1.tar.gz")
	print("Download Hadoop 3.3.1 Successful...")

print("Install Hadoop 3.3.1 .....")
os.system("tar -xzf hadoop-3.3.1.tar.gz -C /usr/local/ && ln -s /usr/local/hadoop-3.3.1/ /usr/local/hadoop")
print("Finished Install Hadoop 3.3.1....")
		  
print("Config Hadoop 3.3.1 ...")
os.system("sed -i '/export JAVA_HOME/s/${JAVA_HOME}/\/usr\/lib\/jvm\/default-java\//g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_NAMENODE_USER=root' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_DATANODE_USER=root' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_SECONDARYNAMENODE_USER=root' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")
os.system("echo 'export YARN_RESOURCEMANAGER_USER=root' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")
os.system("echo 'export YARN_NODEMANAGER_USER=root' >>  /usr/local/hadoop-3.3.1/etc/hadoop/hadoop-env.sh")

os.system("echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_NAMENODE_USER=root' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_DATANODE_USER=root' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export HDFS_SECONDARYNAMENODE_USER=root' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export YARN_RESOURCEMANAGER_USER=root' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
os.system("echo 'export YARN_NODEMANAGER_USER=root' >>  /usr/local/hadoop/etc/hadoop/hadoop-env.sh")
	  
os.system("echo 'export HADOOP_HOME=/usr/local/hadoop' >> /root/.bashrc")
os.system("source ~/.bashrc")
os.system("pip3 install pyhdfs")	 


print("Downloading and install  Scala2.11.12")
os.system("curl https://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz > /scala-2.11.12.tgz && tar -xzf /scala-2.11.12.tgz -C /usr/local/ && ln -s /usr/local/scala-2.11.12 /usr/local/scala")
print("Finished install scala-2.11.12")

if not os.path.exists("/spark-examples/spark-3.2.1-bin-hadoop3.2.tgz"):
	print("Downloading Spark 3.2.1....")
	os.system("curl https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz > /spark-examples/spark-3.2.1-bin-hadoop3.2.tgz")	
	print("Download Spark 3.2.1 Successful...")

print("Install Spark 3.2.1 .....")
os.system("tar -xzf /spark-examples/spark-3.2.1-bin-hadoop3.2.tgz -C /usr/local/ && ln -s /usr/local/spark-3.2.1-bin-hadoop3.2/ /usr/local/spark")
print("Finished Install Spark 3.3.1....")


#config env
os.system("echo 'export JAVA_HOME=/usr/lib/jvm/default-java/' >> /root/.bashrc")
os.system("echo 'export SCALA_HOME=/usr/local/scala' >> /root/.bashrc")
os.system("echo 'export PATH=$PATH:/usr/local/hadoop/bin/:/usr/local/hadoop/sbin/:/usr/local/scala/bin' >> /root/.bashrc")
#config ssh
os.system("echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config")
#config dir
os.system("mkdir -p /data/hadoop/node && mkdir -p /data/hadoop/data && mkdir -p /data/hadoop/name")
print("Config Hadoop 3.3.1 ...")
os.system("sed -i '/export JAVA_HOME/s/${JAVA_HOME}/\/usr\/lib\/jvm\/default-java\//g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh")

#core-site.xml
coreSiteXml = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
         <name>fs.defaultFS</name>
         <value>hdfs://%(mip)s:9000</value>
    </property>
    <property>
	<name>dfs.namenode.rpc-bind-host</name>
	<value>0.0.0.0</value>
    </property>
</configuration>""" % dict(mip=mip)
writeHadoopConfigFile("core-site.xml",coreSiteXml)
hdfsSiteXml = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>


    <property>  
       <name>dfs.permissions</name>  
      <value>false</value>  
   </property>
 
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:50070</value>
    </property>
 
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>0.0.0.0:50090</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/data/hadoop/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/data/hadoop/data</value>
    </property>
</configuration>
"""
writeHadoopConfigFile("hdfs-site.xml",hdfsSiteXml)

mapredSiteXml = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
"""
writeHadoopConfigFile("mapred-site.xml",mapredSiteXml)

yarnSiteXml = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>%(mip)s</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <property>
         <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
         <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>%(mip)s:8032</value>
  </property>
  <property>
     <name>yarn.resourcemanager.scheduler.address</name>
     <value>%(mip)s:8030</value>
  </property>
  <property>
     <name>yarn.resourcemanager.resource-tracker.address</name>
     <value>%(mip)s:8031</value>
  </property>
  <property>
     <name>yarn.resourcemanager.admin.address</name>
     <value>0.0.0.0:8033</value>
   </property>
   <property>
      <name>yarn.resourcemanager.webapp.address</name>
      <value>0.0.0.0:8088</value>
   </property>
</configuration>
""" % dict(mip=mip)
writeHadoopConfigFile("yarn-site.xml",yarnSiteXml)

writeHadoopConfigFile("master",mip)
writeHadoopConfigFile("slaves",sip)

envConfig = """export JAVA_HOME=/usr/lib/jvm/default-java/
export SCALA_HOME=/usr/local/scala
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_MASTER_IP=%(mip)s
export SPARK_MASTER_HOST=%(mip)s
export SPARK_WORKER_MEMORY=1g
export SPARK_WORKER_CORES=2
export SPARK_HOME=/usr/local/spark
export SPARK_DIST_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
""" % dict(mip=mip)

writeSparkConfigFile("spark-env.sh",envConfig)
writeSparkConfigFile("slaves",sip)

#format hdfs
os.system("/usr/local/hadoop/bin/hdfs namenode -format")
