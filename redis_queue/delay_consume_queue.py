import json
import time
from datetime import datetime
from threading import Thread

import client_builder
from redis_queue import RedisEvent


class DelayConsumeQueue:
    def __init__(
        self,
        queue_name: str,
    ):
        self.__redis_client = client_builder.get_redis_client()
        # 任务队列
        self.__task_queue_name = queue_name

        self.__task_result_key_prefix = f"{queue_name}_task_result"

    def push_event(self, event_type: str, params: str, execute_timestamp: int):
        event = RedisEvent.build(event_type, params, self.__task_result_key_prefix)
        self.get_redis_client().zadd(
            self.__task_queue_name, {json.dumps(event.model_dump()): execute_timestamp}
        )

    def __run_delay_queue(self):
        while True:
            time.sleep(1)
            now_timestamp = int(datetime.now().timestamp())

            cmd = self.get_redis_client().register_script(
                """
                local values = redis.call("zrangebyscore", KEYS[1], ARGV[1], ARGV[2])
                redis.call("zremrangebyscore", KEYS[1], ARGV[1], ARGV[2])
                return values
                """
            )

            data_list = cmd(keys=[self.__task_queue_name], args=[0, now_timestamp])

            if not data_list:
                continue

            for data in data_list:
                event = RedisEvent.model_validate(json.loads(data))
                print(event)

    def listen(self):
        Thread(target=self.__run_delay_queue).start()

    def get_redis_client(self):
        return self.__redis_client
