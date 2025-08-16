#!/usr/bin/env bash
set -euo pipefail
mkdir -p jars
cd jars

ICEBERG_VER=1.6.1
SPARK_SCALA=3.5_2.12
HADOOP_AWS_VER=3.3.4
AWS_BUNDLE_VER=1.12.262

download () {
  local url="$1"
  local file="${2:-}"
  if [ -z "$file" ]; then file="$(basename "$url")"; fi
  if [ -f "$file" ]; then
    echo "Exists: $file"
  else
    echo "Downloading: $file"
    curl -L -o "$file" "$url"
  fi
}

download "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_SCALA}/${ICEBERG_VER}/iceberg-spark-runtime-${SPARK_SCALA}-${ICEBERG_VER}.jar"
download "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar"
download "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VER}/aws-java-sdk-bundle-${AWS_BUNDLE_VER}.jar"

echo "Done. JARs placed in ./jars"
