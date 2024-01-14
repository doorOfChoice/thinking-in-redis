import uuid
import time
import threading

import redis

redis_client = redis.StrictRedis(decode_responses=True)

queue_name = "queue_name"


class ProducerThread(threading.Thread):
    def run(self) -> None:
        while True:
            time.sleep(1)
            redis_client.lpush(queue_name, str(uuid.uuid4()))


class ConsumerThread(threading.Thread):
    def run(self) -> None:
        while True:
            data = redis_client.brpop([queue_name], timeout=0)
            print(data)


if __name__ == '__main__':
    ConsumerThread().start()
    ProducerThread().start()
