#!/bin/sh
# Read the env variable $SECRET_{KEY_FROM_CONTAINER_CONFIG} to see where it is mounted

export GOOGLE_APPLICATION_CREDENTIALS=$(echo ${SECRET_GOOGLE_BQ_AUTH})

. /opt/.venv/bin/activate && exec python /opt/main/main.py
