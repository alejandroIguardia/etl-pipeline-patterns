# ⚡ ETL Pipeline Patterns — SSIS, SQL Server & SAP HANA

> A curated collection of ETL patterns, optimization techniques, and engineering practices I've used in production to build reliable, fast, observable data pipelines. Focused on the Microsoft stack (SSIS + SQL Server) with SAP HANA as a common source system.

---

## 📌 Why this exists

Most ETL tutorials show you how to drag boxes in SSIS and load a CSV. That's not the hard part. The hard part is:

- Pipelines that don't break at 3am
- Loads that finish in 20 minutes instead of 4 hours
- Failures that fail loudly and recoverably
- Code (yes, ETL is code) you can read 6 months later

This repo documents how I think about those problems.

---

## 📚 What's inside

### 🔥 Pattern 1: Incremental loads (the most important pattern)

Full loads are easy to write and expensive to run. Most real-world warehouses need **incremental loads** — only pulling what changed since the last successful run.

**Approach:**

1. Maintain a `etl_watermarks` control table tracking the last `updated_at` (or LSN) processed per source table.
2. SSIS package reads watermark → builds a parameterized query like:
   ```sql
   SELECT * FROM source_table 
   WHERE UpdateDate > @last_watermark 
     AND UpdateDate <= @run_start_time
   ```
3. After successful load, update watermark to `@run_start_time` (not `MAX(UpdateDate)` — avoids race conditions during long-running loads).
4. Use `@run_start_time` from the start of the run for upper bound — ensures repeatability.

**Why this matters:**
- Reduced a nightly partner-sync from 45 min → 4 min
- Idempotent (rerunning doesn't double-load)
- Naturally handles late-arriving updates

**Gotcha:** make sure source timestamps are reliable. SAP HANA's `MODIFY_TIMESTAMP` and SQL Server's triggers behave differently. Test both with manual updates.

---

### 🛡️ Pattern 2: Validation checkpoints

Don't push bad data into the warehouse. Every load goes through 4 layers:

```
Source → Staging → Validation → Warehouse → Marts
```

1. **Staging:** raw data, exactly as it arrived. No transforms. Truncated and reloaded each run.
2. **Validation:** runs business rule checks (FK validity, required fields, value ranges, duplicate detection). Records that fail go to `rejected_records` with reason codes.
3. **Warehouse:** only validated data merges into dimensional tables.
4. **Marts:** denormalized read models built on top of the warehouse for BI tools.

**Concrete checks I always include:**

- Primary key uniqueness in staging
- Required fields non-null where the business expects them
- Foreign key existence (does this `customer_id` actually exist in dim_customer?)
- Value range sanity (no negative quantities on sale lines, dates within ±10 years of today)
- Row count delta vs. previous run (fail loudly if today's load is 50% smaller than yesterday — usually means source pipeline is broken)

---

### 📈 Pattern 3: SQL optimization that actually moves the needle

Most "slow queries" aren't actually complex queries — they're poorly indexed simple queries. Things I check before adding more CPU:

#### ✅ Index strategy for ETL workloads

- **Source extractions:** make sure there's an index on the column used as watermark (often `UpdateDate`). Without it, every incremental load does a full table scan.
- **Lookups in transforms:** SSIS Lookup transforms cache data in memory by default — for large dimensions, switch to partial cache and add a covering index on the lookup key.
- **Merge operations:** the target of a `MERGE` statement should have an index on the merge keys. Otherwise, you're scanning the entire table for every batch.

#### ✅ Batch sizing

- Default SSIS data flow buffer size (10MB) is too small for modern hardware. Bump it to 50-100MB.
- `DefaultBufferMaxRows` default of 10K rows is also conservative. Test with 50K-100K depending on row width.
- For OLE DB destinations, use **fast load** with table lock + batch size matching buffer size. Difference between 100K rows/sec and 5K rows/sec.

#### ✅ Common anti-patterns I look for

- ❌ `SELECT *` from sources (pulling unnecessary columns wastes IO)
- ❌ Sort transforms in the data flow (do it in SQL with `ORDER BY` + index, not in memory)
- ❌ Row-by-row OLE DB Command in a data flow (use a set-based SQL Execute task instead)
- ❌ Lookups against huge dimensions with full cache (memory pressure → spillover)
- ❌ Implicit data type conversions (look for the warning triangles in SSIS — they cost real performance)

**Real example:** rewrote a nightly customer-sync that ran 2h 47min. After applying the patterns above (incremental load + proper indexing + buffer tuning + removing in-flow sorts), same logic ran in **38 minutes**. ~75% reduction.

---

### 🚨 Pattern 4: Failure handling without panic

ETL will fail. The question is: does it fail *cleanly*?

**Failure modes I plan for:**

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Source system unavailable | Connection timeout | Retry with backoff, alert if > N attempts |
| Source schema changed | Metadata diff in pre-flight check | Halt pipeline, alert immediately, no partial loads |
| Validation rule rejected > X% of rows | Post-staging row counter | Halt before warehouse merge, alert, manual review |
| Target table locked | OLE DB error code 1205 | Wait + retry (often resolves itself) |
| Disk full / quota | Pre-flight space check | Halt, alert ops immediately |

**Logging that actually helps:**

Every package writes to a central `etl_run_log` table:
- Package name, version, run ID
- Start time, end time, duration
- Source rows read, rejected, inserted, updated
- Watermark before / after
- Error stack trace (truncated if huge) on failure

Then a simple Power BI dashboard surfaces:
- Yesterday's runs: status grid
- 7-day trend: durations and row counts
- Top failures by package

---

### 🔄 Pattern 5: Repeatable deployments with Liquibase

Schemas change. Pretending they don't is how you end up with prod-vs-dev drift that breaks pipelines.

I version-control:
- All DDL (tables, views, indexes)
- Stored procedures
- ETL control tables (watermarks, configs)
- Reference data (lookup tables)

Every change is a numbered Liquibase changeset. Deploying to a new environment is one command. Rolling back a bad change is one command. Auditing what changed when is one query.

---

## 🛠️ Tech Stack

- **ETL Engine:** SQL Server Integration Services (SSIS) 2019+
- **Target:** SQL Server 2019, Azure SQL Database, Microsoft Fabric (Warehouse)
- **Source systems:** SAP B1 on HANA, REST APIs, flat files
- **Version control:** Liquibase for schema, Git for SSIS packages
- **Observability:** Custom log tables + Power BI dashboards
- **Orchestration:** SQL Server Agent (for SSIS) + N8N (for API-driven flows)

---

## 📂 Repository Structure

```
etl-pipeline-patterns/
├── README.md                          ← you are here
├── patterns/
│   ├── 01-incremental-loads.md        ← watermark patterns deep dive
│   ├── 02-validation-checkpoints.md   ← multi-layer validation
│   ├── 03-sql-optimization.md         ← real benchmarks & before/after
│   ├── 04-failure-handling.md         ← logging, alerting, recovery
│   └── 05-liquibase-workflow.md       ← schema version control
├── sql-snippets/
│   ├── watermark-control-table.sql
│   ├── run-log-table.sql
│   └── common-validation-queries.sql
└── ssis-conceptual/                   ← annotated screenshots, no proprietary data
```

---

## 🤝 About me

**Roberto Iguardia** — Data Engineer specialized in ETL, ERP integration, and Power BI analytics. 3+ years in production.

- 🌐 [alejandroiguardia.netlify.app](https://alejandroiguardia.netlify.app/)
- 💼 [LinkedIn](https://www.linkedin.com/in/alejandro-iguardia-data-engineer)
- 📧 robertalerosales12@gmail.com

PRs, issues, and "I did it differently" stories welcome.
