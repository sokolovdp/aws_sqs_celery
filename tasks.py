import time
from datetime import timedelta

import redis
from celery import Celery

from celery.schedules import crontab
from celery.task import periodic_task

# celery -A tasks worker/beat --loglevel=info

# request is <class 'celery.app.task.Context'>

app = Celery('tasks',
             backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')


# @app.task(name='tasks.add')
# def add(x, y):
#     total = x + y
#     print('{} + {} = {}'.format(x, y, total))
#     time.sleep(3)
#     return total


# def backoff(attempts):
#     """
#     1, 2, 4, 8, 16, 32...
#     """
#     return 2 ** attempts


# from celery.exceptions import SoftTimeLimitExceeded
# @app.task(bind=True, max_retries=10, soft_time_limit=5)
# def data_extractor(self):
#     try:
#         for i in range(1, 11):
#             print('Crawling HMTL DOM!')
#             if i == 5:
#                 time.sleep(10)
#                 raise ValueError('Crawling Index Error')
#     except SoftTimeLimitExceeded:
#         print('SOFT LIMIT EXIT !!!!!!!!!!!!')
#         exit()
#     except Exception as exc:
#         print('There was an exception lets retry one more time!')
#         raise self.retry(exc=exc, countdown=backoff(self.request.retries))


# @periodic_task(bind=True, run_every=timedelta(seconds=3), name="tasks.send_mail_from_queue_simple")
# def send_mail_from_queue_simple(self):
#     # no invoker is required!
#     try:
#         messages_sent = "example.email"
#         print("{} Email message successfully sent, [{}]".format(self.request.hostname, messages_sent))
#     finally:
#         print("release resources")


# @periodic_task(run_every=(crontab(day_of_week='sunday', minute='*/1')),
#                name="tasks.send_mail_in_queue_task",
#                ignore_result=True)
# def send_mail_queue():
#     try:
#         messages_sent = "example.email"
#         print("Total email message successfully sent %d.", messages_sent)
#     finally:
#         print("release resources")


REDIS_LOCK_KEY = "4088587A2CAB44FD902D6D5C98CD2B17"


@periodic_task(bind=True, run_every=timedelta(seconds=3), name="tasks.send_mail_from_queue")
def send_mail_from_queue(self):
    redis_client = redis.Redis()
    timeout = 60 * 2  # Lock expires in 5 minutes
    have_lock = False
    my_lock = redis_client.lock(REDIS_LOCK_KEY, timeout=timeout)
    try:
        have_lock = my_lock.acquire(blocking=False)
        if have_lock:
            messages_sent = "example.email"
            print("{} Email message successfully sent, [{}]".format(self.request.hostname, messages_sent))
            time.sleep(9)
    finally:
        print("release resources")
        if have_lock:
            my_lock.release()
