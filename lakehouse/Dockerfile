FROM alexmerced/spark33-notebook
WORKDIR /app
COPY lakehouse/spark_ivy_backup /home/docker/.ivy2

USER root
RUN chown -R docker:docker /home/docker/.ivy2 && chmod -R 777 /home/docker/.ivy2
USER docker

COPY lakehouse/scripts/00_Data_ingestion.py lakehouse/scripts/spark_test_config.py /app/
COPY lakehouse/datasets /app/datasets

ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/00_Data_ingestion.py"]




