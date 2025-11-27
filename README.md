# PipelineDataCleaning

This project contains a small PySpark pipeline and a Streamlit dashboard to inspect the cleaned movie dataset.

Repository layout
- `pipeline.py` — main PySpark pipeline. Reads CSVs from `data/input`, cleans and joins them, and writes a single CSV to `data/output/movies_final.csv` (overwrites existing file).
- `data/input/` — place your source CSVs here (TMDB, Letterboxd). The pipeline auto-detects CSV files by name.
- `data/output/movies_final.csv` — final cleaned dataset produced by the pipeline.
- `dashboard.py` — Streamlit dashboard to visualize `movies_final.csv`.
- `requirements.txt` — Python dependencies for running the pipeline and dashboard.

Quick start (Windows PowerShell)

1. Activate the virtual environment (if you have one):
```powershell
.\DataCleaning_env\Scripts\Activate.ps1
```

2. Install dependencies (only needed once):
```powershell
pip install -r requirements.txt
```

3. Run the pipeline to produce the cleaned CSV:
```powershell
python .\pipeline.py
```

4. Run the dashboard (opens at http://localhost:8501):
```powershell
streamlit run dashboard.py
```

Notes and behavior
- The pipeline expects the CSV inputs to be manually placed in `data/input/`.
- The pipeline filters out records without a Letterboxd rating and removes films with tiny budgets/revenues (default thresholds: 1000). You can adjust these thresholds in `pipeline.py`.
- The final CSV is overwritten each run (no backups are created by default).
- The Streamlit dashboard loads the CSV using Spark and converts it to pandas for display. This is fine for modest datasets (the current dataset is ~8k rows). For very large datasets, consider adding server-side pagination or writing summarized results.

If you want changes (different filters, deterministic deduplication, extra charts), tell me which behavior you'd like and I can update the pipeline or dashboard.
# PipelineDataCleaning