web: gunicorn aggregator:app --workers 3 --timeout=1200 --log-file -
worker: celery -A tasks.celery worker --beat --loglevel=info