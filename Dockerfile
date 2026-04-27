FROM quay.io/astronomer/astro-runtime:11.0.0

# Create processed data directory (gitignored, so not in repo)
RUN mkdir -p /usr/local/airflow/data/processed
