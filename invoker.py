import time
#from tasks import data_extractor  # add,
from celery.result import AsyncResult

# data_extractor.delay()

# result = add.delay(1, 2)
#
# while True:
#     _result2 = AsyncResult(result.task_id)
#     status = _result2.status
#     print(status)
#     if 'SUCCESS' in status:
#         print('result after 5 sec wait {_result2}'.format(_result2=_result2.get()))
#         break
#     time.sleep(5)
