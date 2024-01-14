import json
import time
import uuid
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel
from redis import Redis


class RedisEvent(BaseModel):
    """
    统一放置在redis队列中的元素
    """

    event_id: str
    event_type: str
    created_time: int
    params: str
    event_result_key_prefix: str

    @classmethod
    def build(
        cls, event_type: str, params: str, event_result_key_prefix: str
    ) -> "RedisEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            params=params,
            created_time=int(datetime.now().timestamp()),
            event_result_key_prefix=event_result_key_prefix,
        )

    @property
    def event_result_key_name(self):
        return f"{self.event_result_key_prefix}:{self.event_id}"


class IRedisEventHandler(ABC):
    """
    处理不同event_type的消息的handler
    """

    @abstractmethod
    def handle(self, event: RedisEvent) -> Any:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def event_type(cls) -> str:
        raise NotImplementedError()


class RedisEventResult(BaseModel):
    event_id: str
    result: Optional[str] = None

    @classmethod
    def build(cls, event_id: str, result: Optional[str]) -> "RedisEventResult":
        return cls(event_id=event_id, result=result)

class RedisEventFeature:
    def __init__(self, event_result_key_name: str, redis_client: Redis):
        self.event_result_key_name = event_result_key_name
        self.redis_client = redis_client

    def get(self, timeout: int = 60) -> RedisEventResult:
        now_timestamp = int(datetime.now().timestamp())
        max_timestamp = now_timestamp + timeout
        while now_timestamp < max_timestamp:
            value = self.redis_client.get(self.event_result_key_name)
            if not value:
                time.sleep(0.1)
                now_timestamp = int(datetime.now().timestamp())
                continue
            return RedisEventResult.model_validate(json.loads(value))
        raise TimeoutError("获取数据超时")