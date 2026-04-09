# Stage 1 Testing & Verification Guide

This guide walks you through verifying the Stage 1 pipeline step by step.
Follow every step in order. Each step tells you exactly what to run, what
output to expect, and what it means if something looks wrong.

---

## Prerequisites

Before you start, make sure you have:

1. **Docker Desktop** running (check with `docker info`)
2. **DuckDB** installed (`brew install duckdb` on macOS)
3. **The base Docker image** loaded:
   ```bash
   docker image inspect nedbank-de-challenge/base:1.0 > /dev/null 2>&1 && echo "OK" || echo "MISSING - load the base image first"
   ```
4. **Test data** in place. You need these three files somewhere on your machine:
   - `customers.csv` (~80,000 rows)
   - `accounts.csv` (~100,000 rows)
   - `transactions.jsonl` (~1,000,000 rows)

---

## Step 1: Set Up Your Test Data Directory

Create a clean directory structure that mirrors what the scoring system mounts
inside the Docker container.

```bash
# Pick a working directory (adjust path to suit you)
TEST_DIR="/tmp/stage1-test"

# Create the directory structure
mkdir -p "$TEST_DIR/input" "$TEST_DIR/output" "$TEST_DIR/config"

# Copy your data files into the input directory
cp /path/to/customers.csv   "$TEST_DIR/input/"
cp /path/to/accounts.csv    "$TEST_DIR/input/"
cp /path/to/transactions.jsonl "$TEST_DIR/input/"

# Copy the pipeline config
cp config/pipeline_config.yaml "$TEST_DIR/config/"
```

**Verify it looks right:**
```bash
ls -lh "$TEST_DIR/input/"
```
You should see three files: `accounts.csv`, `customers.csv`, `transactions.jsonl`.

---

## Step 2: Build the Docker Image

From the root of the repository (where the `Dockerfile` lives):

```bash
cd /path/to/novation_data_eningeering
docker build -t my-submission:test .
```

**What to look for:**
- The build should end with `Successfully tagged my-submission:test`
- No errors during `pip install` or the Delta JAR pre-download step
- If it fails, read the error message carefully. Common issues:
  - Missing base image (see Prerequisites)
  - Network error during build (you need internet for `docker build`)

---

## Step 3: Run the Pipeline With Full Scoring Constraints

This command replicates exactly what the scoring system does. Every flag
matters.

```bash
# Clean the output directory first
rm -rf "$TEST_DIR/output"/*

# Run with the exact same flags the scorer uses
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL \
  --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$TEST_DIR/input:/data/input:ro" \
  -v "$TEST_DIR/config:/data/config:ro" \
  -v "$TEST_DIR/output:/data/output:rw" \
  my-submission:test

echo "EXIT CODE: $?"
```

**What to look for:**
- The last line of pipeline output should say `Pipeline complete - exit 0`
- `EXIT CODE: 0` printed after the container exits
- Pipeline should complete in under 2 minutes on the Stage 1 dataset
- You will see some WARN messages from Spark (hostname resolution, hadoop
  native library, WindowExec partitioning). These are harmless.

**If it fails:**
- Exit code `137` = out of memory (OOM killed). Pipeline uses too much RAM.
- Exit code `1` = Python exception. Check the traceback in the output.
- Exit code `124` = timed out (only if you wrapped it in `timeout`).

---

## Step 4: Check the Output Directory Structure

The scoring system checks that `bronze/`, `silver/`, and `gold/` directories
were created.

```bash
echo "--- Bronze ---"
ls "$TEST_DIR/output/bronze/"

echo "--- Silver ---"
ls "$TEST_DIR/output/silver/"

echo "--- Gold ---"
ls "$TEST_DIR/output/gold/"
```

**Expected output:**
```
--- Bronze ---
accounts    customers    transactions

--- Silver ---
accounts    customers    transactions

--- Gold ---
dim_accounts    dim_customers    fact_transactions
```

Each of those subdirectories is a Delta table. Verify they contain both
a `_delta_log/` folder and at least one `.parquet` file:

```bash
ls "$TEST_DIR/output/gold/fact_transactions/"
```

**Expected:** You should see a `_delta_log/` directory and one or more
`part-*.gz.parquet` files. The `_delta_log/` folder is what makes it a Delta
table (not just plain Parquet).

---

## Step 5: Verify Gold Tables Are Readable by DuckDB

The scoring system uses DuckDB with the Delta extension to read your output.
This is the most important check.

```bash
# Set the gold path variable for convenience
GOLD="$TEST_DIR/output/gold"

duckdb -c "
INSTALL delta; LOAD delta;
SELECT 'fact_transactions' AS tbl, COUNT(*) AS rows FROM delta_scan('$GOLD/fact_transactions')
UNION ALL
SELECT 'dim_accounts',      COUNT(*) FROM delta_scan('$GOLD/dim_accounts')
UNION ALL
SELECT 'dim_customers',     COUNT(*) FROM delta_scan('$GOLD/dim_customers');
"
```

**Expected output (approximate):**

| tbl                | rows      |
|--------------------|-----------|
| fact_transactions  | 1,000,000 |
| dim_accounts       | 100,000   |
| dim_customers      | 80,000    |

If DuckDB throws an error here, your Delta tables are not valid and the
scoring system will give zero correctness points.

---

## Step 6: Verify Schema Conformance

The spec requires exact field counts and field order. This is worth 5 points.

### 6a. dim_customers (must have exactly 9 fields)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM delta_scan('$GOLD/dim_customers'));
"
```

**Expected (in this exact order):**

| #  | column_name  | column_type |
|----|--------------|-------------|
| 1  | customer_sk  | BIGINT      |
| 2  | customer_id  | VARCHAR     |
| 3  | gender       | VARCHAR     |
| 4  | province     | VARCHAR     |
| 5  | income_band  | VARCHAR     |
| 6  | segment      | VARCHAR     |
| 7  | risk_score   | INTEGER     |
| 8  | kyc_status   | VARCHAR     |
| 9  | age_band     | VARCHAR     |

**Key check:** There must be NO `dob` column. The spec requires `age_band`
to be derived from `dob`, not a copy of it.

### 6b. dim_accounts (must have exactly 11 fields)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM delta_scan('$GOLD/dim_accounts'));
"
```

**Expected (in this exact order):**

| #  | column_name       | column_type    |
|----|-------------------|----------------|
| 1  | account_sk        | BIGINT         |
| 2  | account_id        | VARCHAR        |
| 3  | customer_id       | VARCHAR        |
| 4  | account_type      | VARCHAR        |
| 5  | account_status    | VARCHAR        |
| 6  | open_date         | DATE           |
| 7  | product_tier      | VARCHAR        |
| 8  | digital_channel   | VARCHAR        |
| 9  | credit_limit      | DECIMAL(18,2)  |
| 10 | current_balance   | DECIMAL(18,2)  |
| 11 | last_activity_date| DATE           |

**Key check:** `customer_id` must be at position 3. This field comes from
`accounts.csv.customer_ref` renamed in the Gold layer (GAP-026 fix).

### 6c. fact_transactions (must have exactly 15 fields)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM delta_scan('$GOLD/fact_transactions'));
"
```

**Expected (in this exact order):**

| #  | column_name           | column_type |
|----|-----------------------|-------------|
| 1  | transaction_sk        | BIGINT      |
| 2  | transaction_id        | VARCHAR     |
| 3  | account_sk            | BIGINT      |
| 4  | customer_sk           | BIGINT      |
| 5  | transaction_date      | DATE        |
| 6  | transaction_timestamp | TIMESTAMP   |
| 7  | transaction_type      | VARCHAR     |
| 8  | merchant_category     | VARCHAR     |
| 9  | merchant_subcategory  | VARCHAR     |
| 10 | amount                | DECIMAL(18,2) |
| 11 | currency              | VARCHAR     |
| 12 | channel               | VARCHAR     |
| 13 | province              | VARCHAR     |
| 14 | dq_flag               | VARCHAR     |
| 15 | ingestion_timestamp   | TIMESTAMP   |

**Key checks:**
- `merchant_subcategory` (position 9) must exist and be nullable (all NULL in
  Stage 1 - it only gets populated in Stage 2)
- `dq_flag` (position 14) must exist and be nullable (all NULL in Stage 1)

---

## Step 7: Run the Three Validation Queries

These are the exact queries the scoring system runs. They are worth 15 points
total (5 each).

### Query 1: Transaction Volume by Type (5 points)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT
    transaction_type,
    COUNT(*) AS record_count,
    SUM(amount) AS total_amount
FROM delta_scan('$GOLD/fact_transactions')
GROUP BY transaction_type
ORDER BY transaction_type;
"
```

**Expected:** Exactly 4 rows with these transaction types:
- CREDIT
- DEBIT
- FEE
- REVERSAL

If you see fewer than 4 rows, a transaction type was dropped. If counts
seem way off from what you'd expect, check your dedup logic.

### Query 2: Zero Unlinked Accounts (5 points)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT COUNT(*) AS unlinked_accounts
FROM delta_scan('$GOLD/dim_accounts') AS a
LEFT JOIN delta_scan('$GOLD/dim_customers') AS c
  ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
"
```

**Expected:** Exactly `0`. Every account must link to a customer.

If this returns a number > 0, either:
- `customer_ref` was not renamed to `customer_id` in dim_accounts
- Some customer records were dropped during Silver/Gold transformation
- The join key doesn't match between the two tables

### Query 3: Province Distribution (5 points)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT
    c.province,
    COUNT(DISTINCT a.account_id) AS account_count
FROM delta_scan('$GOLD/dim_accounts') AS a
JOIN delta_scan('$GOLD/dim_customers') AS c
  ON a.customer_id = c.customer_id
GROUP BY c.province
ORDER BY c.province;
"
```

**Expected:** Exactly 9 rows, one for each South African province:
- Eastern Cape
- Free State
- Gauteng
- KwaZulu-Natal
- Limpopo
- Mpumalanga
- North West
- Northern Cape
- Western Cape

If you see fewer than 9, a province was dropped or misspelled.

---

## Step 8: Additional Data Quality Checks

These aren't scored directly by the validation queries, but the scoring
system checks them as part of the schema and correctness evaluation.

### 8a. Currency Standardisation

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT currency, COUNT(*) AS cnt
FROM delta_scan('$GOLD/fact_transactions')
GROUP BY currency;
"
```

**Expected:** Only one row: `ZAR` with a count matching your total
transaction count. The spec says all currency values must be standardised
to `"ZAR"` regardless of source variants.

### 8b. Surrogate Key Uniqueness

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT
    'transaction_sk' AS sk, COUNT(*) - COUNT(DISTINCT transaction_sk) AS dupes
FROM delta_scan('$GOLD/fact_transactions')
UNION ALL
SELECT 'account_sk', COUNT(*) - COUNT(DISTINCT account_sk)
FROM delta_scan('$GOLD/dim_accounts')
UNION ALL
SELECT 'customer_sk', COUNT(*) - COUNT(DISTINCT customer_sk)
FROM delta_scan('$GOLD/dim_customers');
"
```

**Expected:** All dupes = 0. Surrogate keys must be unique.

### 8c. Foreign Key Integrity

```bash
duckdb -c "
INSTALL delta; LOAD delta;
-- Every fact row's account_sk must exist in dim_accounts
SELECT 'orphan_account_sk' AS chk, COUNT(*) AS cnt
FROM delta_scan('$GOLD/fact_transactions') f
LEFT JOIN delta_scan('$GOLD/dim_accounts') a ON f.account_sk = a.account_sk
WHERE a.account_sk IS NULL

UNION ALL

-- Every fact row's customer_sk must exist in dim_customers
SELECT 'orphan_customer_sk', COUNT(*)
FROM delta_scan('$GOLD/fact_transactions') f
LEFT JOIN delta_scan('$GOLD/dim_customers') c ON f.customer_sk = c.customer_sk
WHERE c.customer_sk IS NULL;
"
```

**Expected:** Both counts = 0. Zero orphaned fact rows.

### 8d. Age Band Values

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT age_band, COUNT(*) AS cnt
FROM delta_scan('$GOLD/dim_customers')
GROUP BY age_band
ORDER BY age_band;
"
```

**Expected:** Only these values: `18-25`, `26-35`, `36-45`, `46-55`,
`56-65`, `65+`. No NULLs (given the data generation constraints).

### 8e. DQ Flag (Stage 1)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT dq_flag, COUNT(*) AS cnt
FROM delta_scan('$GOLD/fact_transactions')
GROUP BY dq_flag;
"
```

**Expected:** One row: `NULL` with count = total transactions.
In Stage 1 there are no DQ issues injected, so all records should be clean.

### 8f. Merchant Subcategory (Stage 1)

```bash
duckdb -c "
INSTALL delta; LOAD delta;
SELECT merchant_subcategory, COUNT(*) AS cnt
FROM delta_scan('$GOLD/fact_transactions')
GROUP BY merchant_subcategory;
"
```

**Expected:** One row: `NULL` with count = total transactions.
This field is absent from Stage 1 source data and only gets populated in Stage 2.

---

## Step 9: Run the Official Test Harness

The starter kit includes `run_tests.sh` which runs Checks 1-5 automatically.

```bash
bash infrastructure/run_tests.sh \
  --stage 1 \
  --data-dir "$TEST_DIR" \
  --image my-submission:test
```

**Expected results:**
- Check 1 (image exists): PASS
- Check 2 (exits 0): PASS
- Check 3 (output dirs exist): PASS
- Check 4 (DuckDB reads Gold tables): see note below
- Check 5 (validation queries): see note below

### Known Issue: Check 4 and 5 on DuckDB >= 1.0

If you have DuckDB v1.0 or newer installed on your Mac, Checks 4 and 5 may
show `[FAIL]` even though your output is correct. This is because the test
harness parses DuckDB output with `grep -E '^[0-9]+$'`, which expects a plain
number on its own line. Newer DuckDB versions output results in a table format
with box-drawing characters, so the grep never matches.

**This is a test harness parsing issue, not a pipeline issue.** The actual
scoring system uses its own DuckDB setup and will read your tables correctly.

To confirm your output is valid despite the test harness failures, run the
manual DuckDB checks in Steps 5-8 above. If those all pass, your pipeline
is correct.

---

## Step 10: Idempotency Check

The spec requires surrogate keys to be "stable across pipeline re-runs on the
same input data." Run the pipeline a second time and compare.

```bash
# Save first run's row counts
duckdb -c "
INSTALL delta; LOAD delta;
SELECT 'run1' AS run, COUNT(*) AS txn FROM delta_scan('$GOLD/fact_transactions');
"

# Run pipeline again (same input, same output dir)
docker run --rm \
  --network=none --memory=2g --memory-swap=2g --cpus=2 \
  --pids-limit=512 --read-only --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$TEST_DIR/input:/data/input:ro" \
  -v "$TEST_DIR/config:/data/config:ro" \
  -v "$TEST_DIR/output:/data/output:rw" \
  my-submission:test

# Compare second run
duckdb -c "
INSTALL delta; LOAD delta;
SELECT 'run2' AS run, COUNT(*) AS txn FROM delta_scan('$GOLD/fact_transactions');
"
```

**Expected:** Same row counts across both runs. The pipeline uses
`mode("overwrite")` so each run replaces the previous output cleanly.

---

## Step 11: Monitor Resource Usage (Optional but Recommended)

The scoring system awards efficiency points for staying below 80% of the
2 GB memory limit (i.e., under 1.6 GB peak).

Open a second terminal and run:

```bash
# Start the pipeline in the background
docker run --name stage1-monitor \
  --network=none --memory=2g --memory-swap=2g --cpus=2 \
  --pids-limit=512 --read-only --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "$TEST_DIR/input:/data/input:ro" \
  -v "$TEST_DIR/config:/data/config:ro" \
  -v "$TEST_DIR/output:/data/output:rw" \
  my-submission:test &

# Watch memory usage in the other terminal
docker stats stage1-monitor
```

**What to look for:** Peak MEM USAGE should stay well under 2.00 GiB.
Below 1.6 GiB earns you efficiency credit.

---

## Step 12: Verify Git State Before Tagging

Before creating the submission tag, verify your repository is clean and
contains everything the scorer expects.

```bash
# Check nothing is uncommitted
git status

# Verify required files exist
ls Dockerfile pipeline/run_all.py pipeline/ingest.py pipeline/transform.py \
   pipeline/provision.py config/pipeline_config.yaml requirements.txt README.md
```

**All of these must exist.** The scoring system checks for them.

Also verify `output/` is in `.gitignore`:
```bash
grep "output/" .gitignore
```

---

## Step 13: Create the Submission Tag and Push

```bash
# Create annotated tag
git tag -a stage1-submission -m "Stage 1 submission"

# Push tag to remote
git push origin stage1-submission

# Verify the tag is visible on the remote
git ls-remote origin refs/tags/stage1-submission
```

**Expected:** The `git ls-remote` command should print a commit hash. If it
prints nothing, the tag was not pushed.

---

## Quick Reference: Scoring Breakdown

| Dimension       | Weight | What the scorer checks                                      |
|-----------------|--------|-------------------------------------------------------------|
| Correctness     | 40%    | Validation queries pass, schema matches, field counts exact |
| Scalability     | 25%    | Completes in time, stays within memory, execution time rank |
| Maintainability | 20%    | Config externalised, modules separable, structured for change |
| Efficiency      | 15%    | No `.collect()` on large frames, peak memory < 80%, no redundant scans |

**Minimum to advance:** 50 points out of 100.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Exit code 137 | OOM killed | Reduce Spark memory config, check for `.toPandas()` on large frames |
| Exit code 1 | Python exception | Read the traceback in the Docker output |
| "No module named 'pipeline'" | PYTHONPATH not set | Verify `ENV PYTHONPATH=/app` in Dockerfile |
| "ClassNotFoundException: DeltaSparkSessionExtension" | Delta JARs not on classpath | Verify the JAR pre-download step in Dockerfile |
| DuckDB "could not read Delta table" | Missing `_delta_log/` | Verify you write with `.format("delta")` not `.format("parquet")` |
| 0 rows in a Gold table | Join dropped all rows | Check join keys match between Silver tables |
| Query 2 returns > 0 | `customer_ref` not renamed | Verify `.withColumnRenamed("customer_ref", "customer_id")` in provision.py |
| Fewer than 9 provinces | Province data dropped | Check dedup logic isn't too aggressive |
| Fewer than 4 transaction types | Type filter or bad dedup | Verify Silver layer preserves all transaction types |
