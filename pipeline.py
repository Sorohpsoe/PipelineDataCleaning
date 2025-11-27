##############################################################
##############################################################
# Movie Data Pipeline - CSC1142
#
# This PySpark script:
# - downloads datasets (if needed)
# - loads the "Millions of Movies"   dataset
# - loads a Letterboxd dataset
# - cleans and normalizes movie titles
# - cleans budget and revenue values
# - normalizes genres
# - joins both datasets (only movies present in both)
# - exports a final analytics dataset:
#       [title, genres, letterboxd_rating, budget, revenue]
##############################################################

from pathlib import Path
import sys
import os
import shutil
import time
import glob

from pyspark.sql import SparkSession
import csv
from pyspark.sql.functions import (
    lower, regexp_replace, trim, col, lit, when,
    regexp_extract,
    avg, first, max, round as spark_round
)


##############################################################
# Helper function to clean movie titles
##############################################################

def clean_title(df, source_column_name):
    return df.withColumn(
        "title_clean",
        trim(
            regexp_replace(
                lower(col(source_column_name)),
                r"[^a-z0-9 ]",
                ""  # keep only letters, digits and spaces
            )
        )
    )

def find_csv_path(keywords, base_dir=Path("data") / "input"):
    """Search recursively under base_dir for a CSV whose filename contains any keyword.

    Returns the first matching Path or None.
    """
    base = Path(base_dir)
    if not base.exists():
        return None

    keywords = [k.lower() for k in keywords]
    for p in base.rglob("*.csv"):
        name = p.name.lower()
        if any(k in name for k in keywords):
            return p
    # fallback: any CSV
    all_csvs = list(base.rglob("*.csv"))
    return all_csvs[0] if all_csvs else None


def pick_column(cols, candidates):
    cols_low = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in cols_low:
            # return actual column name (case preserved)
            return cols[cols_low.index(cand.lower())]
    return None


##############################################################
# Main pipeline function
##############################################################


def main():

    # Datasets are expected to be placed manually in `data/input`.
    # The automatic downloader step has been disabled.

    ##########################################################
    # 1. Initialize Spark
    ##########################################################
    spark = SparkSession.builder.appName("Movies_Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ##########################################################
    # 2. Discover CSV files under data/input
    ##########################################################
    movies_csv = find_csv_path(["millions", "million", "movies"])
    letterboxd_csv = find_csv_path(["letterboxd", "letterbox", "lb", "movies"])

    # If both searches returned the same file, try to pick the correct files
    # when multiple CSVs exist in `data/input`. Prefer explicit names like
    # 'tmdb' for the movies dataset and 'letterboxd' for the letterboxd dataset.
    if movies_csv and letterboxd_csv and movies_csv == letterboxd_csv:
        base = Path("data") / "input"
        all_csvs = list(base.rglob("*.csv"))
        # try exact heuristics first
        tmdb = next((p for p in all_csvs if 'tmdb' in p.name.lower()), None)
        lb = next((p for p in all_csvs if 'letterboxd' in p.name.lower()), None)
        if tmdb and lb:
            movies_csv = tmdb
            letterboxd_csv = lb
            print("Info: using", movies_csv, "for movies and", letterboxd_csv, "for letterboxd")
        else:
            # fallback: pick any different CSV for letterboxd
            for p in all_csvs:
                if p != movies_csv:
                    # prefer a file whose name contains 'movie' or 'millions' for movies
                    if 'movie' in p.name.lower() or 'millions' in p.name.lower():
                        movies_csv = p
                        continue
                    letterboxd_csv = p
                    print("Warning: same file matched both datasets; using", letterboxd_csv, "for letterboxd")
                    break

    if not movies_csv or not letterboxd_csv:
        print("Could not find required CSV files under data/input.")
        print("Found:")
        print("  movies:", movies_csv)
        print("  letterboxd:", letterboxd_csv)
        print("Make sure datasets are downloaded into data/input and try again.")
        sys.exit(1)

    print("Using movies CSV:", movies_csv)
    print("Using letterboxd CSV:", letterboxd_csv)

    ##########################################################
    # 3. Load datasets
    ##########################################################
    movies = spark.read.csv(str(movies_csv), header=True, inferSchema=True)
    letterboxd = spark.read.csv(str(letterboxd_csv), header=True, inferSchema=True)

    print("Movies rows:", movies.count())
    print("Letterboxd rows:", letterboxd.count())

    ##########################################################
    # 4. Determine title & rating columns dynamically
    ##########################################################
    movies_title_col = pick_column(movies.columns, ["title", "name", "movie", "original_title"]) or movies.columns[0]
    letterboxd_title_col = pick_column(letterboxd.columns, ["film", "title", "name"]) or letterboxd.columns[0]
    rating_col = pick_column(letterboxd.columns, ["rating", "score"])  # may be None

    print(f"Using movies title column: {movies_title_col}")
    print(f"Using letterboxd title column: {letterboxd_title_col}")
    if rating_col:
        print(f"Using letterboxd rating column: {rating_col}")
    else:
        print("No rating column detected in letterboxd CSV; `letterboxd_rating` will be empty")

    ##########################################################
    # 5. Clean title columns
    ##########################################################
    movies = clean_title(movies, movies_title_col)
    letterboxd = clean_title(letterboxd, letterboxd_title_col)

    ##########################################################
    # 6. Clean budget & revenue (convert to integers)
    ##########################################################
    # Parse numeric budget & revenue robustly: tolerate decimals like '123.0'
    # and malformed values (set to NULL). Use LONG to avoid overflow on large values.
    num_regex = r'^[0-9]+(\.[0-9]+)?$'

    if "budget" in movies.columns:
        movies = movies.withColumn(
            "budget_clean",
            when(col("budget").cast("string").rlike(num_regex),
                 col("budget").cast("double").cast("long")
                 ).otherwise(lit(None).cast("long"))
        ).drop("budget").withColumnRenamed("budget_clean", "budget")
    else:
        movies = movies.withColumn("budget", lit(None).cast("long"))

    if "revenue" in movies.columns:
        movies = movies.withColumn(
            "revenue_clean",
            when(col("revenue").cast("string").rlike(num_regex),
                 col("revenue").cast("double").cast("long")
                 ).otherwise(lit(None).cast("long"))
        ).drop("revenue").withColumnRenamed("revenue_clean", "revenue")
    else:
        movies = movies.withColumn("revenue", lit(None).cast("long"))

    # Optional: remove movies with missing or zero financial data if columns exist
    # Exclude movies with missing or extremely small financials. Define
    # reasonable minimum thresholds so that tiny/placeholder values are ignored.
    BUDGET_MIN = 1000   # euros/dollars (adjust if you want a different floor)
    REVENUE_MIN = 1000

    if "budget" in movies.columns and "revenue" in movies.columns:
        movies = movies.filter(
            (col("budget").isNotNull()) & (col("budget") >= BUDGET_MIN) &
            (col("revenue").isNotNull()) & (col("revenue") >= REVENUE_MIN)
        )
        print(f"Filtering movies with budget>={BUDGET_MIN} and revenue>={REVENUE_MIN}")

    ##########################################################
    # 7. Normalize genres (replace | with comma) if present
    ##########################################################
    if "genres" in movies.columns:
        movies = movies.withColumn("genres", regexp_replace(col("genres"), r"\|", ", "))
    else:
        # ensure genres has a STRING type (avoid VOID/NULL type that Spark CSV writer rejects)
        movies = movies.withColumn("genres", lit(None).cast("string"))

    ##########################################################
    # 8. Join datasets (only movies present in both)
    # Alias dataframes so column references are unambiguous after the join
    ##########################################################
    # Try to detect date/year columns in both dataframes so we can avoid merging
    # different films that only share the same title.
    movie_date_col = pick_column(movies.columns, ["release_date", "released", "release", "year", "date"])
    lb_date_col = pick_column(letterboxd.columns, ["release_date", "released", "release", "year", "date"])

    if movie_date_col and lb_date_col:
        # build normalized year columns from free-form date fields
        # prefer exact 4-digit year, fallback to extracting first 4-digit group
        # safer year extraction: extract a 4-digit group and only cast when present
        m_extracted = regexp_extract(col(movie_date_col).cast("string"), r"([0-9]{4})", 1)
        movies = movies.withColumn(
            "year",
            when(col(movie_date_col).rlike(r'^[0-9]{4}$'), col(movie_date_col).cast("int"))
            .when(m_extracted.rlike(r'^[0-9]{4}$'), m_extracted.cast("int"))
            .otherwise(lit(None).cast("int"))
        )
        l_extracted = regexp_extract(col(lb_date_col).cast("string"), r"([0-9]{4})", 1)
        letterboxd = letterboxd.withColumn(
            "year",
            when(col(lb_date_col).rlike(r'^[0-9]{4}$'), col(lb_date_col).cast("int"))
            .when(l_extracted.rlike(r'^[0-9]{4}$'), l_extracted.cast("int"))
            .otherwise(lit(None).cast("int"))
        )
        print(f"Detected date columns: movies->{movie_date_col}, letterboxd->{lb_date_col}; joining on title_clean+year")
        join_on = ["title_clean", "year"]
    else:
        print("Date columns not detected in both datasets; joining on title_clean only")
        join_on = "title_clean"

    movies = movies.alias("m")
    letterboxd = letterboxd.alias("l")

    joined = movies.join(letterboxd, on=join_on, how="inner")
    print("Movies present in both datasets:", joined.count())

    ##########################################################
    # 9. Select final output columns
    ##########################################################
    # Clean and validate letterboxd rating: numeric, decimal dot normalized,
    # then ensure values are between 0 and 5 (otherwise NULL).
    if rating_col:
        raw_rating = trim(regexp_replace(col(f"l.{rating_col}").cast("string"), ",", "."))
        # accept simple numeric formats like 3.5 or 4
        rating_valid = raw_rating.rlike(num_regex)
        rating_val = when(rating_valid, raw_rating.cast("double")).otherwise(lit(None).cast("double"))
        rating_expr = when((rating_val.isNotNull()) & (rating_val.between(0.0, 5.0)), rating_val).otherwise(lit(None).cast("double")).alias("letterboxd_rating")
    else:
        rating_expr = lit(None).cast("double").alias("letterboxd_rating")

    final = joined.select(
        col(f"m.{movies_title_col}").alias("title"),
        col("m.genres"),
        rating_expr,
        col("m.budget"),
        col("m.revenue")
    )

    # Remove rows with no Letterboxd rating (we only want movies with a rating)
    final = final.filter(col("letterboxd_rating").isNotNull())

    final.show(20, truncate=False)

    ##########################################################
    # 9.5 Remove duplicate titles (keep first occurrence)
    ##########################################################
    # Use dropDuplicates on the `title` column to remove exact-title duplicates.
    # Note: dropDuplicates keeps an arbitrary row for each title (first encountered).
    final = final.dropDuplicates(["title"])
    print("Rows after deduplication:", final.count())

    ##########################################################
    # 10. Export the final dataset as a single CSV in data/output
    ##########################################################
    out_dir = Path("data") / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    final_file = out_dir / "movies_final.csv"

    # If a previous final file exists, remove it so the new file overwrites it
    if final_file.exists():
        try:
            final_file.unlink()
        except Exception:
            try:
                os.remove(str(final_file))
            except Exception:
                # If removal fails, proceed — later move will overwrite if possible
                pass

    # Try to write a single CSV using Spark (coalesce to 1 partition),
    # then move the produced part file into the desired final path.
    # Remove stray backslashes and normalize internal double-quotes in titles
    final = final.withColumn("title", regexp_replace(col("title"), r"\\\\", ""))
    # Replace literal double-quote characters with single quote to avoid complex CSV escaping
    final = final.withColumn("title", regexp_replace(col("title"), '"', "'"))

    tmp_dir = out_dir / f"tmp_spark_write_{int(time.time())}"
    # Write a single-part CSV with Spark and move the produced part file to the final location
    # Write with options to prefer doubled quotes rather than backslash-escapes
    final.coalesce(1).write\
        .option("header", True)\
        .option("quote", '"')\
        .option("escape", '"')\
        .option("escapeQuotes", False)\
        .csv(str(tmp_dir), mode="overwrite")
    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise RuntimeError("Spark produced no part-*.csv file in temporary output")
    part = part_files[0]
    # move the part file to final location
    if final_file.exists():
        final_file.unlink()
    shutil.move(str(part), str(final_file))
    # cleanup temporary directory
    shutil.rmtree(str(tmp_dir), ignore_errors=True)
    print("Final dataset written to:", final_file)

    ##########################################################
    # 11. Stop Spark
    ##########################################################
    spark.stop()


##############################################################
# Entry point
##############################################################

if __name__ == "__main__":
    main()
