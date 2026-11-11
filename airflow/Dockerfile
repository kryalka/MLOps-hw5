FROM apache/airflow:2.10.5-python3.11
USER airflow
COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/project-requirements.txt
RUN pip install --no-cache-dir "cloudpickle==2.2.1" "joblib==1.4.2" && pip check

USER root
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh
ENTRYPOINT ["/custom-entrypoint.sh"]
