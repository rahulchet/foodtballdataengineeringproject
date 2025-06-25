#!/bin/bash
# Entrypoint to initialize Airflow and create default user

airflow db upgrade

airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email chetwanirahul@gmail.com\
  --password admin

exec airflow webserver