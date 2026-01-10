# French National Address Database - Incremental Storage System

A production-ready Apache Spark application that efficiently stores daily snapshots of the French National Address Database (Base Adresse Nationale) using incremental change detection and Parquet storage format.

## Table of Contents

- [Problem & Solution](#problem--solution)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Compilation](#compilation)
  - [Docker Setup](#docker-setup)
- [Usage](#usage)
  - [1. Daily File Integration](#1-daily-file-integration)
  - [2. Daily Report](#2-daily-report)
  - [3. Recompute Dump at Date](#3-recompute-dump-at-date)
  - [4. Compute Diff Between Files](#4-compute-diff-between-files)
- [Testing](#testing)
  - [Integration Test Suite](#integration-test-suite)
  - [Expected Data Structure](#expected-data-structure)
- [Professor Integration Test](#professor-integration-test)
- [Implementation Details](#implementation-details)
- [Limitations & Future Improvements](#limitations--future-improvements)

## Problem & Solution

### The Challenge
The French BAL (Base Adresse Locale) dataset is available as a daily snapshot at approximately 1.4 GB compressed CSV. Storing complete daily dumps for long-term retention (30+ years) would require:

```
30 years × 365 days × 2 GB = 21,900 GB ≈ 21 Petabytes
```

This naive approach is economically unfeasible.

### The Solution
This project implements a **Change Data Capture (CDC)** strategy using Apache Spark:

- **Incremental Storage**: Only INSERT, UPDATE, and DELETE operations are stored daily
- **Parquet Format**: Efficient columnar storage with compression
- **Hive Partitioning**: Data partitioned by day for optimal query performance
- **SHA-256 Hashing**: Detect data changes by comparing cryptographic hashes

**Data Structure:**
```
bal.db/
├── bal_latest/          # Current complete snapshot (overwritten daily)
└── bal_diff/            # Incremental changes partitioned by day
    ├── day=2025-01-01/
    ├── day=2025-01-02/
    └── ...
```

Daily incremental files are expected to be only a few megabytes instead of gigabytes, making 30+ year retention practical.

## Tech Stack

- **Language**: Java 11
- **Framework**: Apache Spark 3.5.0 (spark-sql)
- **Build Tool**: Maven 3.x
- **Storage Format**: Parquet with Hive partitioning
- **Data Source**: CSV with semicolon delimiter (`;`)
- **Primary Key**: `cle_interop` (unique address identifier)
- **Hash Algorithm**: SHA-256 for change detection

## Project Structure

```
spark-project/
├── pom.xml                          # Maven configuration
├── docker-compose.yml               # Docker environment setup
├── Dockerfile                       # Spark development container
├── scripts/                         # Shell scripts for job execution
│   ├── run_daily_file_integration.sh
│   ├── run_report.sh
│   ├── recompute_and_extract_dump_at_date.sh
│   ├── compute_diff_between_files.sh
│   └── test.sh                      # Integration test suite
├── data/                            # Sample CSV files
│   ├── adresses-mini-step1.csv
│   ├── adresses-mini-step2.csv
│   └── adresses-mini-step3.csv
└── src/main/java/fr/esilv/
    ├── SparkMain.java               # Entry point with command routing
    └── jobs/
        ├── IntegrationJob.java      # Daily incremental integration
        ├── ReportJob.java           # Statistics and reports
        ├── RecomputeJob.java        # Reconstruct snapshot at date
        └── DiffJob.java             # Compare two parquet datasets
```

### Job Descriptions

1. **IntegrationJob**: Ingests daily CSV files, computes differences (INSERT/UPDATE/DELETE), and stores incremental changes
2. **ReportJob**: Generates statistical reports on the latest data (addresses per department, totals, etc.)
3. **RecomputeJob**: Reconstructs the complete address database as of a specific historical date
4. **DiffJob**: Compares two Parquet datasets and outputs differences

## Getting Started

### Prerequisites

- Java 11 or higher
- Apache Maven 3.x
- Apache Spark 3.5.0 (for local execution)
- Docker & Docker Compose (optional, for containerized environment)

### Compilation

Build the project with Maven:

```bash
mvn clean install
```

This generates the uber JAR: `target/spark-project-1.0-SNAPSHOT.jar`

### Docker Setup

For a containerized Spark environment with all dependencies pre-installed:

```bash
# Start the container
docker compose up -d

# Access the container shell
docker exec -it spark_project_env bash

# Inside the container, compile and run
mvn clean install
./scripts/test.sh
```

The container includes:
- Eclipse Temurin JDK 11
- Apache Maven
- Apache Spark 3.5.0
- Mounted volumes for project code and data

To stop the container:

```bash
docker compose down
```

## Usage

All jobs are executed via `spark-submit` through shell scripts located in the `scripts/` directory.

### 1. Daily File Integration

Integrates a CSV file for a specific date, computing and storing incremental changes.

**Script:** `run_daily_file_integration.sh`

**Syntax:**
```bash
./scripts/run_daily_file_integration.sh <date> <csvFile>
```

**What it does:**
- Reads the CSV file with semicolon delimiter
- Computes SHA-256 hash for each record
- Compares with previous snapshot (`bal_latest`)
- Identifies INSERT, UPDATE, DELETE operations
- Stores differences in `bal.db/bal_diff/day=<date>/`
- Updates `bal.db/bal_latest/` with the new snapshot

**Output:**
```
=== Integration Job ===
Date: 2025-01-15
CSV File: /data/addresses-2025-01-15.csv
Insertions: 1523
Deletions: 47
Updates: 892
=== Integration Job Completed Successfully ===
```

### 2. Daily Report

Generates aggregate statistics on the latest address data.

**Script:** `run_report.sh`

**Syntax:**
```bash
./scripts/run_report.sh
```

**What it does:**
- Reads the current snapshot from `bal_latest`
- Computes total addresses and communes
- Aggregates statistics by department (département)
- Displays top 10 departments by address count

**Sample Output:**
```
=== BAL Report Job ===

=== Global Statistics ===
Total addresses: 245789
Total communes: 1847

=== Top 10 Departments ===
+------------+----------+---------+
|departement |addresses |communes |
+------------+----------+---------+
|75          |89234     |20       |
|69          |45678     |293      |
|13          |42156     |119      |
...
+------------+----------+---------+
```

### 3. Recompute Dump at Date

Reconstructs the complete address database as it existed on a specific historical date.

**Script:** `recompute_and_extract_dump_at_date.sh`

**Syntax:**
```bash
./scripts/recompute_and_extract_dump_at_date.sh <date> <outputDir>
```

**What it does:**
- Reads all incremental changes up to the target date from `bal_diff`
- Applies changes chronologically
- For each address (`cle_interop`), keeps only the latest operation
- Excludes DELETEd addresses
- Outputs the reconstructed snapshot to the specified directory

**Output:**
```
=== Recompute Job ===
Target date: 2025-01-10
Output directory: /output/snapshot-2025-01-10
Total diffs to process: 15234
Final address count at 2025-01-10: 243567
Dump saved to: /output/snapshot-2025-01-10
=== Recompute Job Completed Successfully ===
```

### 4. Compute Diff Between Files

Compares two Parquet datasets and identifies differences.

**Script:** `compute_diff_between_files.sh`

**Syntax:**
```bash
./scripts/compute_diff_between_files.sh <parquetDir1> <parquetDir2>
```

**What it does:**
- Loads both Parquet datasets
- Computes hash values if not present
- Identifies INSERT operations (in dataset2 but not dataset1)
- Identifies DELETE operations (in dataset1 but not dataset2)
- Identifies UPDATE operations (same key but different hash)
- Saves results to `bal.db/diff_output/`

**Output:**
```
=== Diff Job ===
Parquet Dir 1: /output/snapshot-2025-01-01
Parquet Dir 2: /output/snapshot-2025-01-15
Insertions: 1523
Deletions: 47
Updates: 892
Diff saved to: bal.db/diff_output
=== Diff Job Completed Successfully ===
```

## Testing

### Integration Test Suite

The `test.sh` script validates all four features with sample data:

```bash
./scripts/test.sh
```

**Test Workflow:**
1. Compiles the project with Maven
2. Cleans the database directory
3. Runs three daily integrations with sample CSV files
4. Generates a report
5. Recomputes three historical snapshots
6. Compares the snapshots pairwise

**Console Output Example:**

```bash
$ ./scripts/test.sh

[INFO] Scanning for projects...
[INFO] Building spark-project 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- maven-clean-plugin:2.5:clean (default-clean) @ spark-project ---
[INFO] Deleting target
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.1:compile (default-compile) @ spark-project ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 5 source files to target/classes
[INFO] 
[INFO] --- maven-shade-plugin:3.2.4:shade (default) @ spark-project ---
[INFO] Including org.apache.spark:spark-sql_2.12:jar:3.5.0 in the shaded jar.
[INFO] Replacing original artifact with shaded artifact.
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------

=== Integration Job ===
Date: 2025-01-01
CSV File: data/adresses-mini-step1.csv
Insertions: 5
Deletions: 0
Updates: 0
=== Integration Job Completed Successfully ===

=== Integration Job ===
Date: 2025-01-02
CSV File: data/adresses-mini-step2.csv
Insertions: 2
Deletions: 0
Updates: 1
=== Integration Job Completed Successfully ===

=== Integration Job ===
Date: 2025-01-03
CSV File: data/adresses-mini-step3.csv
Insertions: 1
Deletions: 1
Updates: 2
=== Integration Job Completed Successfully ===

=== BAL Report Job ===

=== Global Statistics ===
Total addresses: 7
Total communes: 4

=== Top 10 Departments ===
+------------+----------+---------+
|departement |addresses |communes |
+------------+----------+---------+
|75          |3         |2        |
|69          |2         |1        |
|13          |2         |1        |
+------------+----------+---------+

=== Report Completed Successfully ===

=== Recompute Job ===
Target date: 2025-01-01
Output directory: bal.db/recompute_2025-01-01
Total diffs to process: 5
Final address count at 2025-01-01: 5
Dump saved to: bal.db/recompute_2025-01-01
=== Recompute Job Completed Successfully ===

=== Recompute Job ===
Target date: 2025-01-02
Output directory: bal.db/recompute_2025-01-02
Total diffs to process: 8
Final address count at 2025-01-02: 7
Dump saved to: bal.db/recompute_2025-01-02
=== Recompute Job Completed Successfully ===

=== Recompute Job ===
Target date: 2025-01-03
Output directory: bal.db/recompute_2025-01-03
Total diffs to process: 11
Final address count at 2025-01-03: 7
Dump saved to: bal.db/recompute_2025-01-03
=== Recompute Job Completed Successfully ===

=== Diff Job ===
Parquet Dir 1: bal.db/recompute_2025-01-01
Parquet Dir 2: bal.db/recompute_2025-01-02
Insertions: 2
Deletions: 0
Updates: 1
=== Diff Job Completed Successfully ===

=== Diff Job ===
Parquet Dir 1: bal.db/recompute_2025-01-01
Parquet Dir 2: bal.db/recompute_2025-01-03
Insertions: 2
Deletions: 1
Updates: 2
=== Diff Job Completed Successfully ===

=== Diff Job ===
Parquet Dir 1: bal.db/recompute_2025-01-02
Parquet Dir 2: bal.db/recompute_2025-01-03
Insertions: 0
Deletions: 1
Updates: 1
=== Diff Job Completed Successfully ===
```

### Expected Data Structure

After running the test suite:

```
bal.db/
├── bal_latest/                    # Current snapshot (7 addresses)
│   └── part-00000-*.parquet
├── bal_diff/                      # Incremental changes
│   ├── day=2025-01-01/
│   │   └── part-00000-*.parquet   # 5 INSERTs
│   ├── day=2025-01-02/
│   │   └── part-00000-*.parquet   # 2 INSERTs, 1 UPDATE
│   └── day=2025-01-03/
│       └── part-00000-*.parquet   # 1 INSERT, 1 DELETE, 2 UPDATEs
├── recompute_2025-01-01/          # Reconstructed snapshot
├── recompute_2025-01-02/
├── recompute_2025-01-03/
└── diff_output/                   # Comparison results
```

## Professor Integration Test

To run the complete integration test with 50 daily dumps (2025-01-01 to 2025-02-20):

**Prerequisites:** Place the 50 CSV dump files in `c:/data/` as:
```
c:/data/dump-2025-01-01
c:/data/dump-2025-01-02
...
c:/data/dump-2025-02-20
```

**Execution:**

```bash
#!/bin/bash

# Compile and clean
mvn clean install
rm -rf bal.db

# Process 50 days
for n in $(seq 1 50); do 
    day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
    echo "Processing day ${n}: ${day}"
    ./scripts/run_daily_file_integration.sh ${day} c:/data/dump-${day}
    ./scripts/run_report.sh
done

# Recompute and compare
./scripts/recompute_and_extract_dump_at_date.sh 2025-01-24 c:/temp/dumpA
./scripts/recompute_and_extract_dump_at_date.sh 2025-02-10 c:/temp/dumpB
./scripts/compute_diff_between_files.sh c:/temp/dumpA c:/temp/dumpB
```

**Expected Result:**

```
bal.db/
├── bal_latest/              # Final snapshot (2025-02-20)
├── bal_diff/                # 50 daily partitions
│   ├── day=2025-01-02/
│   ├── day=2025-01-03/
│   ...
│   └── day=2025-02-20/
└── diff_output/             # Comparison results

c:/temp/
├── dumpA/                   # Recomputed at 2025-01-24
└── dumpB/                   # Recomputed at 2025-02-10
```

## Implementation Details

### Change Detection with SHA-256 Hashing

To detect modifications without comparing every column, the system computes a SHA-256 hash of all columns:

```java
// Sort columns alphabetically for consistency
String[] sortedColumns = Arrays.stream(dataset.columns())
        .sorted()
        .toArray(String[]::new);

// Concatenate all columns with delimiter and hash
dataset.withColumn("hash_value", 
    sha2(concat_ws("||", sortedColumns), 256));
```

This allows efficient comparison: if hashes differ, the record has been modified.

### Partitioning Strategy

Data is partitioned by the `day` column using Hive-style partitioning:

```java
dataset.write()
    .mode(SaveMode.Append)
    .partitionBy("day")
    .parquet(BAL_DIFF_PATH);
```

Benefits:
- Efficient time-range queries
- Partition pruning for better performance
- Easy to manage retention policies (delete old partitions)

### First Run vs Incremental Runs

**First Run:**
- No previous data exists (`bal_latest` is empty)
- All records are marked as INSERT
- Establishes the initial baseline

**Incremental Runs:**
- Compares new data with `bal_latest`
- Detects INSERT (new keys), DELETE (missing keys), UPDATE (same key, different hash)
- Stores only the differences

**Data Source:** [French National Address Database (BAL)](https://adresse.data.gouv.fr/data/ban/adresses/latest/csv-bal/adresses-france.csv.gz)
