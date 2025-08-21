from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import re
from google.cloud import bigquery
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression

# -------- Step 2: Preprocess Headlines --------
def preprocess_news():
    client = bigquery.Client()
    query = "SELECT * FROM sentiment_demo.raw_news"
    df = client.query(query).to_dataframe()

    # Rename columns if CSV has no headers
    if "title" not in df.columns:
        df.rename(columns={
            "string_field_0": "title",
            "string_field_1": "sentiment"
        }, inplace=True)

    # Basic text cleaning
    df["clean_headline"] = df["title"].apply(lambda x: re.sub(r'[^a-zA-Z ]', '', str(x).lower()))

    # Write to BigQuery
    table_id = "sentiment_demo.processed_news"
    client.load_table_from_dataframe(df, table_id).result()


# -------- Step 3: Train + Predict Sentiment --------
def run_sentiment_model():
    client = bigquery.Client()
    query = "SELECT clean_headline, sentiment FROM sentiment_demo.processed_news"
    df = client.query(query).to_dataframe()

    # Train simple Logistic Regression model
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(df["clean_headline"])
    y = df["sentiment"]

    model = LogisticRegression(max_iter=200)
    model.fit(X, y)

    # Predict sentiment
    df["predicted_sentiment"] = model.predict(X)

    # Store predictions
    table_id = "sentiment_demo.predictions"
    client.load_table_from_dataframe(df, table_id).result()


# -------- Define DAG --------
with DAG(
    dag_id="financial_news_sentiment_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["financial", "composer", "ml"],
) as dag:

    # Step 1: Ingestion (CSV → BigQuery)
    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="asia-southeast1-financial-s-93184d44-bucket",
        source_objects=["all-data.csv"],  # CSV without headers
        destination_project_dataset_table="sentiment_demo.raw_news",
        source_format="CSV",
        skip_leading_rows=0,  # read all rows as data
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # Step 2: Preprocess text
    preprocess = PythonOperator(
        task_id="preprocess_text",
        python_callable=preprocess_news,
    )

    # Step 3: Train + Predict sentiment
    predict = PythonOperator(
        task_id="ml_predict",
        python_callable=run_sentiment_model,
    )

    # Define dependencies
    load_csv >> preprocess >> predict
