WORKER_NAME = 'CeleryFlow'

# event
TASK_MAX_RETRIES = 1
TASK_COUNTDOWN = 3
MAX_RESEND_TIMES = 2

# celery
broker_url = 'amqp://guest:guest@localhost:5672'
task_serializer = 'json'
result_serializer = 'json'
worker_prefetch_multiplier = 0
worker_concurrency = 8
