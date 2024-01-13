import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Thread

from pydantic import BaseModel

import client_maker


class RedisEvent(BaseModel):
    """
    统一放置在redis队列中的元素
    """

    event_id: str
    event_type: str
    created_time: int
    data: str

    @classmethod
    def build(cls, event_type: str, data: str) -> "RedisEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            data=data,
            created_time=int(datetime.now().timestamp()),
        )


class IRedisEventHandler(ABC):
    """
    处理不同event_type的消息的handler
    """

    @abstractmethod
    def handle(self, event: RedisEvent):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def event_type(cls) -> str:
        raise NotImplementedError()


class RedisConsumeQueue:
    def __init__(
            self,
            queue_name: str,
            max_retry_interval_sec: int = 60 * 5,
            max_abandon_sec: int = 60 * 20,
    ):
        self.__redis_client = client_maker.get_redis_client()
        # 任务队列
        self.__task_queue_name = queue_name
        # 处理队列
        self.__processing_queue_name = f"{queue_name}_backup"
        # 处理事件的handler
        self.__event_type_handler_map = {}
        # 消息超时时间，超过这个时间将会被重新放入待处理列表
        self.__max_retry_interval_sec = max_retry_interval_sec
        # 消息抛弃时间，超过这个时间未被处理将会被抛弃
        self.__max_abandon_sec = max_abandon_sec

    def __run_normal_queue(self):
        while True:
            element = self.get_redis_client().brpoplpush(
                self.__task_queue_name, self.__processing_queue_name, timeout=0
            )
            event = RedisEvent.model_validate(json.loads(element))
            event_handler = self.get_event_handler(event.event_type)
            success_process = True
            if event_handler:
                try:
                    event_handler.handle(event)
                except Exception as e:
                    success_process = False
                    logging.exception(f"process element:{element} failed")
            if success_process:
                self.get_redis_client().lrem(self.__processing_queue_name, 0, element)

    def __run_failed_queue(self):
        while True:
            time.sleep(self.__max_retry_interval_sec)
            now_timestamp = int(datetime.now().timestamp())
            start = 0
            limit = 1000
            data_list = None
            while data_list is None or len(data_list) > 0:
                data_list = self.get_redis_client().lrange(
                    self.__processing_queue_name, start, start + limit
                )
                start += limit
                for data in data_list:
                    event = RedisEvent.model_validate(json.loads(data))
                    diff_time = now_timestamp - event.created_time
                    if diff_time < self.__max_retry_interval_sec:
                        continue
                    elif (
                            self.__max_retry_interval_sec
                            <= diff_time
                            < self.__max_abandon_sec
                    ):
                        logging.info(
                            f"start to retry, data:{data}"
                        )
                        # TODO 这里需要改良, 优化成一个原子操作
                        self.__rem_from_process_queue(data)
                        self.__push_to_queue(data)
                    else:
                        logging.warning(
                            f"retry end, abandon data, data:{data}"
                        )
                        self.__rem_from_process_queue(data)


    def __rem_from_process_queue(self, data: str):
        """
        从处理队列移除数据
        :param data:
        :return:
        """
        self.get_redis_client().lrem(self.__processing_queue_name, 0, data)

    def __push_to_queue(self, data: str):
        """
        推送数据到待处理队列
        :param data:
        :return:
        """
        self.get_redis_client().lpush(self.__task_queue_name, data)

    def listen(self):
        """
        start to listen queue
        :return:
        """
        # process data
        Thread(target=self.__run_normal_queue).start()
        # listen data of failed, try to revoke or abandon
        Thread(target=self.__run_failed_queue).start()

    def push_event(self, event_type: str, content: str):
        """
        向redis发送消息
        :param event_type:
        :param content:
        :return:
        """
        event = RedisEvent.build(event_type, content)
        self.__push_to_queue(json.dumps(event.model_dump()))

    def register_event_handler(self, handler: IRedisEventHandler):
        """
        注册处理redis消息的handler
        每个RedisEvent都会对应一个event_type，需要根据不同的event_type给定不同的处理方式
        :param handler:
        :return:
        """
        self.__event_type_handler_map[handler.event_type()] = handler

    def get_event_handler(self, event_type: str) -> IRedisEventHandler | None:
        return self.__event_type_handler_map.get(event_type)

    def get_redis_client(self):
        return self.__redis_client
