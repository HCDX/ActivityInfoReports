web: NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn aggregator:app --workers 3 --timeout=1200 --log-file -
worker: NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program celery -A tasks.celery worker --beat --loglevel=info