    
import streamlit as st
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

st.set_page_config(page_title="Movies Dashboard", layout="wide")

st.title("Movies Dashboard — data/output/movies_final.csv (Spark)")

DATA_PATH = Path("data") / "output" / "movies_final.csv"


@st.cache_data
def load_data(path: Path):
    if not path.exists():
        return None
    # create Spark session and read CSV via Spark
    spark = SparkSession.builder.appName("Movies_Dashboard").getOrCreate()
    sdf = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    # ensure numeric types exist
    for c in ["letterboxd_rating", "revenue", "budget"]:
        if c in sdf.columns:
            sdf = sdf.withColumn(c, col(c).cast("double"))
    # convert to pandas for Streamlit display (dataset is expected to be reasonably small)
    pdf = sdf.toPandas()
    # stop spark to free resources
    spark.stop()
    return pdf


df = load_data(DATA_PATH)

if df is None:
    st.error(f"File not found: {DATA_PATH}. Run the pipeline first.")
    st.stop()

# Basic metrics
st.sidebar.header("Filter")

min_rating = float(df["letterboxd_rating"].min()) if "letterboxd_rating" in df.columns else 0.0
max_rating = float(df["letterboxd_rating"].max()) if "letterboxd_rating" in df.columns else 5.0
rating_range = st.sidebar.slider("Minimum rating", min_value=0.0, max_value=5.0, value=(min_rating, max_rating), step=0.1)

# build genre list
all_genres = set()
if "genres" in df.columns:
    for g in df["genres"].dropna().astype(str):
        for piece in [s.strip() for s in g.split(",")]:
            if piece:
                all_genres.add(piece)
genres_sorted = sorted(all_genres)
selected_genres = st.sidebar.multiselect("Genres (filter)", options=genres_sorted, default=genres_sorted)

sort_by = st.sidebar.selectbox("Sort by", options=["letterboxd_rating", "revenue", "budget"], index=0)
asc = st.sidebar.checkbox("Ascending", value=False)

st.sidebar.markdown("---")
st.sidebar.write(f"Total source films: {len(df)}")

# Apply filters
df_filtered = df.copy()
if "letterboxd_rating" in df_filtered.columns:
    df_filtered = df_filtered[df_filtered["letterboxd_rating"].between(rating_range[0], rating_range[1])]

if selected_genres:
    # keep rows that contain any of the selected genres
    pattern = "|".join([fr"\b{g}\b" for g in selected_genres])
    df_filtered = df_filtered[df_filtered["genres"].fillna("").str.contains(pattern, regex=True, na=False)]

if sort_by in df_filtered.columns:
    df_filtered = df_filtered.sort_values(by=sort_by, ascending=asc)

st.markdown("**Filtered data preview**")
col1, col2, col3 = st.columns(3)
col1.metric("Displayed films", len(df_filtered))
col2.metric("Average rating", f"{df_filtered['letterboxd_rating'].mean():.2f}" if len(df_filtered) else "N/A")
col3.metric("Average revenue", f"{int(df_filtered['revenue'].mean()):,}" if (len(df_filtered) and 'revenue' in df_filtered.columns) else "N/A")

with st.expander("Table (first rows)"):
    st.dataframe(df_filtered.head(200))

st.markdown("---")

st.markdown("**Rating distribution**")
if "letterboxd_rating" in df_filtered.columns:
    hist = df_filtered["letterboxd_rating"].dropna().value_counts().sort_index()
    st.bar_chart(hist)
else:
    st.info("Aucune colonne `letterboxd_rating` disponible.")

st.markdown("---")
st.markdown("**Top 10 films by revenue**")
if "revenue" in df_filtered.columns:
    top_rev = df_filtered.dropna(subset=["revenue"]).nlargest(10, "revenue")[["title", "revenue", "letterboxd_rating"]]
    st.table(top_rev)
else:
    st.info("Aucune colonne `revenue` disponible.")

st.markdown("---")
