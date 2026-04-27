FROM quay.io/astronomer/astro-runtime:3.2-2

# Create processed data directory (gitignored, so not in repo)
RUN mkdir -p /usr/local/airflow/data/processed
