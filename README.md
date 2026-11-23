# TP3 – Spark SQL: Google N-Gram Analysis

## Overview
This practical assignment introduces Spark SQL and the DataFrame API. It consists of loading the Google Books N-Gram dataset (bi-grams) into a Spark DataFrame, registering it as a temporary view, and answering a series of analytical queries using two approaches:

1. **SQL**: queries written in the SQL language against a temporary view.
2. **DataFrame API**: the equivalent queries written with the Spark SQL API methods.

Each query is implemented both ways so the two approaches can be compared on the same results.

## Technologies
* Apache Spark (Spark SQL)
* PySpark (Python)
* DataFrame API and Window functions
* Google Colab / Docker (execution environments)

## Dataset
The input file `ngram.csv` (tab-separated, no header) comes from the Google N-Gram dataset and contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `ngram` | String | The bi-gram itself |
| `year` | Integer | Year of appearance |
| `count` | Integer | Number of occurrences in books of that year |
| `pages` | Integer | Number of pages containing the bi-gram in that year |
| `books` | Integer | Number of distinct books containing the bi-gram in that year |



## Project Structure
```
.
├── ngram_sparksql.py   # PySpark program (SQL + DataFrame API queries)
└── README.md
```

## Implementation
The program creates a SparkSession, loads the file with an explicit schema (column names and casts), registers the DataFrame as the temporary view `ngrams`, then answers each query with both methods:

| # | Query | Main techniques |
|---|-------|-----------------|
| 1 | Bi-grams with `count` > 5 | `WHERE` / `filter` |
| 2 | Number of bi-grams per year | `GROUP BY` / `groupBy().count()` |
| 3 | Bi-gram(s) with the highest count in each year | Join on `MAX` / `Window` + `dense_rank` |
| 4 | Bi-grams that appear in 20 different years | `COUNT(DISTINCT)` / `countDistinct` |
| 5 | Bi-grams with `!` in the first word and `9` in the second | `split`, `LIKE` / `contains` |
| 6 | Bi-grams that appear in all years of the dataset | `HAVING` with the total number of distinct years |
| 7 | Total pages and books per bi-gram and year, sorted alphabetically | `SUM`, multi-column `GROUP BY`, `ORDER BY` |
| 8 | Number of distinct bi-grams per year, by descending year | `COUNT(DISTINCT)` / `countDistinct`, `desc` |

## Execution
In Google Colab, install PySpark first:
```bash
!pip install pyspark
```

Then place `ngram.csv` at the path used in the script (`file:///root/ngram.csv`, adjust if needed) and run:
```bash
spark-submit ngram_sparksql.py
```

## Learning Outcomes
* Creating DataFrames from CSV files with a defined schema.
* Registering temporary views and querying them with Spark SQL.
* Expressing the same query in SQL and in the DataFrame API.
* Using aggregations, joins, string functions, and window functions.
* Understanding the declarative approach of Spark SQL compared to the RDD API (TP2).

## Academic Context
Practical Work 3 – Big Data
Higher School of Computer Science (ESI-SBA), Algeria
Academic Year: 2026–2027
