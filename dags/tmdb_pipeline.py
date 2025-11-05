from __future__ import annotations
from datetime import datetime
import os, ast, glob
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = "/opt/airflow/data"
RAW_DIR = f"{DATA_DIR}/raw"
BRONZE_DIR = f"{DATA_DIR}/bronze"
SILVER_DIR = f"{DATA_DIR}/silver"
OUTPUTS_DIR = f"{DATA_DIR}/outputs"

default_args = {"owner": "tea", "retries": 0}

with DAG(
    dag_id="tmdb_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tmdb", "ingest", "transform", "load", "analyze"],
    default_args=default_args,
    template_searchpath=["/opt/airflow"],  # so sql="include/..." works
) as dag:

    @task
    def ensure_dirs():
        for d in (BRONZE_DIR, SILVER_DIR, OUTPUTS_DIR):
            os.makedirs(d, exist_ok=True)
        return True

    @task
    def bronze_movies() -> str:
        path_in = f"{RAW_DIR}/tmdb_5000_movies.csv"
        df = pd.read_csv(path_in, low_memory=False)
        df = df[pd.to_numeric(df["id"], errors="coerce").notna()].copy()
        df["id"] = df["id"].astype("int64")
        if "release_date" in df.columns:
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
        for col in ["vote_average","vote_count","popularity","runtime"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        out = f"{BRONZE_DIR}/movies.parquet"
        df.to_parquet(out, index=False)
        return out

    @task
    def bronze_credits() -> str:
        path_in = f"{RAW_DIR}/tmdb_5000_credits.csv"
        df = pd.read_csv(path_in)
        df = df[pd.to_numeric(df["movie_id"], errors="coerce").notna()].copy()
        df["movie_id"] = df["movie_id"].astype("int64")
        out = f"{BRONZE_DIR}/credits.parquet"
        df.to_parquet(out, index=False)
        return out

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_default",
        sql="include/sql/create_tables.sql",
    )

    @task
    def transform_merge() -> str:
        movies = pd.read_parquet(f"{BRONZE_DIR}/movies.parquet")
        credits = pd.read_parquet(f"{BRONZE_DIR}/credits.parquet")

        def parse_list_of_dicts(s):
            if pd.isna(s): return []
            try:
                x = ast.literal_eval(s) if isinstance(s, str) else s
                return x if isinstance(x, list) else []
            except Exception:
                return []

        if "genres" in movies.columns:
            movies["genres_list"] = movies["genres"].apply(parse_list_of_dicts)
            movies["genres_names"] = movies["genres_list"].apply(
                lambda L: ", ".join([d.get("name","") for d in L if isinstance(d, dict) and d.get("name")])
            )

        if "cast" in credits.columns:
            credits["cast_list"] = credits["cast"].apply(parse_list_of_dicts)
            credits["cast_top3"] = credits["cast_list"].apply(
                lambda L: ", ".join([d.get("name","") for d in L if isinstance(d, dict) and d.get("name")][:3])
            )

        mcols = ["id","title","release_date","vote_average","vote_count","popularity","runtime"]
        if "genres_names" in movies.columns:
            mcols.append("genres_names")
        movies_thin = movies[mcols].copy()

        ccols = ["movie_id"]
        if "cast_top3" in credits.columns:
            ccols.append("cast_top3")
        credits_thin = credits[ccols].copy().rename(columns={"movie_id":"id"})

        merged = movies_thin.merge(credits_thin, on="id", how="left")
        merged = merged.rename(columns={"genres_names":"genres"})
        out = f"{SILVER_DIR}/movies_merged.parquet"
        merged.to_parquet(out, index=False)
        return out

    truncate_movies = PostgresOperator(
        task_id="truncate_movies",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE tmdb.movies_final;",
    )

    @task
    def load_to_postgres(silver_path: str):
        df = pd.read_parquet(silver_path)
        expected = ["id","title","release_date","vote_average","vote_count","popularity","runtime","genres","cast_top3"]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            df.to_sql(
                "movies_final", con=conn, schema="tmdb",
                if_exists="append", index=False, method="multi", chunksize=1000,
            )
        return True

    @task
    def analyze_to_csv():
        """Read from Postgres, split genres in pandas (no SQL regex), write CSV."""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        df = hook.get_pandas_df("select id,title,vote_average,vote_count,genres from tmdb.movies_final;")
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        out_csv = f"{OUTPUTS_DIR}/top_genres_by_rating.csv"
        if df.empty:
            pd.DataFrame(columns=["genre","n_movies","avg_vote"]).to_csv(out_csv, index=False)
            return out_csv

        df["genres"] = df["genres"].fillna("")
        rows = []
        for _, r in df.iterrows():
            for g in [x.strip() for x in r["genres"].split(",") if x.strip()]:
                rows.append({"genre": g, "vote_average": r["vote_average"], "vote_count": r["vote_count"]})
        ex = pd.DataFrame(rows)
        if ex.empty:
            pd.DataFrame(columns=["genre","n_movies","avg_vote"]).to_csv(out_csv, index=False)
            return out_csv

        ex = ex[ex["vote_count"] >= 50]
        out = (ex.groupby("genre", as_index=False)["vote_average"]
                 .mean()
                 .rename(columns={"vote_average":"avg_vote"}))
        counts = ex.groupby("genre", as_index=False).size().rename(columns={"size":"n_movies"})
        out = out.merge(counts, on="genre", how="left").sort_values("avg_vote", ascending=False).head(10)
        out[["genre","n_movies","avg_vote"]].to_csv(out_csv, index=False)
        return out_csv

    @task
    def cleanup_intermediate():
        removed = []
        for folder in (BRONZE_DIR, SILVER_DIR):
            for p in glob.glob(os.path.join(folder, "*.parquet")):
                try:
                    os.remove(p); removed.append(p)
                except FileNotFoundError:
                    pass
        return removed

    ok = ensure_dirs()
    m_path = bronze_movies()
    c_path = bronze_credits()

    ok >> [m_path, c_path]
    t_path = transform_merge()
    [m_path, c_path] >> t_path

    create_tables >> truncate_movies
    t_path >> truncate_movies
    loaded = load_to_postgres(t_path)
    loaded >> analyze_to_csv() >> cleanup_intermediate()
