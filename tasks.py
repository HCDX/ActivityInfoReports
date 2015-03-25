__author__ = 'jcranwellward'

from celery import Celery
from celery.schedules import crontab

from manage import app, import_ai


CELERYBEAT_SCHEDULE = {
    # Executes import every 3 hours
    'import-ai-everyday': {
        'task': 'tasks.run_import',
        'schedule': crontab(minute=0, hour='*/3'),
    },
}


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    celery.conf.CELERYBEAT_SCHEDULE = CELERYBEAT_SCHEDULE
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


celery = make_celery(app)


@celery.task
def run_import():
    import_ai('1800,1883,1884,1885,1886,1887,1888,1889,1890',
              username='jcranwellward@unicef.org', password='Inn0vation')