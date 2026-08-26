FROM astrocrpublic.azurecr.io/runtime:3.3-2
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./

# this venv is what Cosmos' VIRTUALENV execution mode 
RUN python3.13 -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

 