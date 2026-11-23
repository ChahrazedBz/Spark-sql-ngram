from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("TP3-SparkSQL-NGram") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


input_path = "file:///root/ngram.csv"


df = spark.read \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .option("sep", "\t") \
    .csv(input_path)

df = df.toDF("ngram", "year", "count", "pages", "books")

print("\n=== Schema ===")
df.printSchema()

print("\n=== Sample ===")
df.show(5, truncate=False)


expected = {"ngram", "year", "count", "pages", "books"}
cols = set([c.lower() for c in df.columns])
if cols != expected:
    df = df.toDF(*[c.lower() for c in df.columns])

df = df.select(
    F.col("ngram").cast("string").alias("ngram"),
    F.col("year").cast("int").alias("year"),
    F.col("count").cast("int").alias("count"),
    F.col("pages").cast("int").alias("pages"),
    F.col("books").cast("int").alias("books")
)

df.createOrReplaceTempView("ngrams")

def show_title(title):
    print("\n" + "="*70)
    print(title)
    print("="*70)

# ---------------------------------------
# Q1) bi-grams with count > 5
# ---------------------------------------
show_title("Q1 SQL: ngrams where count > 5")
spark.sql("""
SELECT *
FROM ngrams
WHERE count > 5
LIMIT 20
""").show(truncate=False)

show_title("Q1 API: ngrams where count > 5")
df.filter(F.col("count") > 5).show(20, truncate=False)

# ---------------------------------------
# Q2) number of bi-grams in each year
# (Interpretation: total rows per year)
# ---------------------------------------
show_title("Q2 SQL: number of bi-grams (rows) per year")
spark.sql("""
SELECT year, COUNT(*) AS nb_bigrams
FROM ngrams
GROUP BY year
ORDER BY year
""").show(50)

show_title("Q2 API: number of bi-grams (rows) per year")
df.groupBy("year").count().withColumnRenamed("count", "nb_bigrams").orderBy("year").show(50)

# ---------------------------------------
# Q3) bi-grams with the biggest count in each year
# (Top 1 per year)
# ---------------------------------------
show_title("Q3 SQL: bigram(s) with max count per year")
spark.sql("""
SELECT n.year, n.ngram, n.count
FROM ngrams n
JOIN (
    SELECT year, MAX(count) AS max_count
    FROM ngrams
    GROUP BY year
) m
ON n.year = m.year AND n.count = m.max_count
ORDER BY n.year
""").show(50, truncate=False)

show_title("Q3 API: bigram(s) with max count per year")
w = Window.partitionBy("year").orderBy(F.desc("count"))
df.withColumn("rn", F.dense_rank().over(w)) \
  .filter(F.col("rn") == 1) \
  .select("year", "ngram", "count") \
  .orderBy("year") \
  .show(50, truncate=False)

# ---------------------------------------
# Q4) bi-grams that appeared in 20 different years
# (Interpretation: ngram has 20 distinct years)
# ---------------------------------------
show_title("Q4 SQL: ngrams appearing in 20 different years")
spark.sql("""
SELECT ngram, COUNT(DISTINCT year) AS nb_years
FROM ngrams
GROUP BY ngram
HAVING COUNT(DISTINCT year) = 20
ORDER BY ngram
LIMIT 50
""").show(50, truncate=False)

show_title("Q4 API: ngrams appearing in 20 different years")
df.groupBy("ngram") \
  .agg(F.countDistinct("year").alias("nb_years")) \
  .filter(F.col("nb_years") == 20) \
  .orderBy("ngram") \
  .show(50, truncate=False)

# ---------------------------------------
# Q5) bi-grams with 'e' in first part and '9' in second part
# "first part" and "second part" are separated by a space
# ---------------------------------------
show_title("Q5 SQL: first token contains '!' AND second token contains '9'")

spark.sql("""
SELECT *
FROM ngrams
WHERE split(ngram, ' ')[0] LIKE '%!%'
  AND split(ngram, ' ')[1] LIKE '%9%'
""").show(50, truncate=False)
show_title("Q5 API: first token contains '!' AND second token contains '9'")

df.filter(
    F.split(F.col("ngram"), " ").getItem(0).contains("!") &
    F.split(F.col("ngram"), " ").getItem(1).contains("9")
).show(50, truncate=False)

# ---------------------------------------
# Q6) bi-grams that appeared in ALL years present in the dataset
# ---------------------------------------
show_title("Q6: ngrams appearing in all years in dataset")
total_years = df.select("year").distinct().count()

# SQL
spark.sql("""
SELECT ngram
FROM ngrams
GROUP BY ngram
HAVING COUNT(DISTINCT year) = {}
ORDER BY ngram
LIMIT 50
""".format(total_years)).show(50, truncate=False)

# API
df.groupBy("ngram") \
  .agg(F.countDistinct("year").alias("nb_years")) \
  .filter(F.col("nb_years") == total_years) \
  .orderBy("ngram") \
  .show(50, truncate=False)

# ---------------------------------------
# Q7) total pages and books per ngram per year, sorted alphabetically by ngram
# ---------------------------------------
show_title("Q7 SQL: sum(pages), sum(books) per (ngram, year) sorted by ngram")
spark.sql("""
SELECT ngram, year, SUM(pages) AS total_pages, SUM(books) AS total_books
FROM ngrams
GROUP BY ngram, year
ORDER BY ngram ASC, year ASC
LIMIT 50
""").show(50, truncate=False)

show_title("Q7 API: sum(pages), sum(books) per (ngram, year) sorted by ngram")
df.groupBy("ngram", "year") \
  .agg(F.sum("pages").alias("total_pages"), F.sum("books").alias("total_books")) \
  .orderBy(F.asc("ngram"), F.asc("year")) \
  .show(50, truncate=False)

# ---------------------------------------
# Q8) number of distinct bi-grams per year, sorted by year descending
# ---------------------------------------
show_title("Q8 SQL: number of distinct ngrams per year (descending year)")
spark.sql("""
SELECT year, COUNT(DISTINCT ngram) AS nb_distinct_bigrams
FROM ngrams
GROUP BY year
ORDER BY year DESC
""").show(50)

show_title("Q8 API: number of distinct ngrams per year (descending year)")
df.groupBy("year") \
  .agg(F.countDistinct("ngram").alias("nb_distinct_bigrams")) \
  .orderBy(F.desc("year")) \
  .show(50)

spark.stop()
