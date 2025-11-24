##############################################################
##############################################################
# Movie Data Pipeline - CSC1142
#
# This PySpark script:
# - downloads datasets (if needed)
# - loads the "Millions of Movies" dataset
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lower, regexp_replace, trim, col, lit
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
    if "budget" in movies.columns:
        movies = movies.withColumn("budget", col("budget").cast("int"))
    else:
        movies = movies.withColumn("budget", lit(None).cast("int"))

    if "revenue" in movies.columns:
        movies = movies.withColumn("revenue", col("revenue").cast("int"))
    else:
        movies = movies.withColumn("revenue", lit(None).cast("int"))

    # Optional: remove movies with missing or zero financial data if columns exist
    if "budget" in movies.columns and "revenue" in movies.columns:
        movies = movies.filter(
            (col("budget").isNotNull()) & (col("budget") > 0) &
            (col("revenue").isNotNull()) & (col("revenue") > 0)
        )

    ##########################################################
    # 7. Normalize genres (replace | with comma) if present
    ##########################################################
    if "genres" in movies.columns:
        movies = movies.withColumn("genres", regexp_replace(col("genres"), r"\|", ", "))
    else:
        movies = movies.withColumn("genres", lit(None))

    ##########################################################
    # 8. Join datasets (only movies present in both)
    ##########################################################
    joined = movies.join(letterboxd, on="title_clean", how="inner")
    print("Movies present in both datasets:", joined.count())

    ##########################################################
    # 9. Select final output columns
    ##########################################################
    rating_expr = col(rating_col).alias("letterboxd_rating") if rating_col else lit(None).alias("letterboxd_rating")

    final = joined.select(
        col(movies_title_col).alias("title"),
        col("genres"),
        rating_expr,
        col("budget"),
        col("revenue")
    )

    final.show(20, truncate=False)

    ##########################################################
    # 10. Export the final dataset
    ##########################################################
    output_path = "output/movies_final"
    final.write.csv(output_path, header=True, mode="overwrite")
    print("Final dataset written to:", output_path)

    ##########################################################
    # 11. Stop Spark
    ##########################################################
    spark.stop()


##############################################################
# Entry point
##############################################################

if __name__ == "__main__":
    main()
