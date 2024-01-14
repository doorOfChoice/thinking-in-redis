from threading import Thread

from redis_queue.consume_queue import IRedisEventHandler, RedisEvent, RedisConsumeQueue


class PrintTextHandler(IRedisEventHandler):
    """
    处理打印消息的Handler
    """

    def handle(self, event: RedisEvent):
        if event.params == "ex":
            raise Exception("ex")
        return event.params + ", this is a test data"

    @classmethod
    def event_type(cls) -> str:
        return "text"


class UserInputThread(Thread):
    """
    用户录入消息的线程
    """

    def __init__(self, queue: RedisConsumeQueue):
        super().__init__()
        self.queue = queue

    def run(self) -> None:
        while True:
            msg = input("input your msg:")
            feature = self.queue.push_event(PrintTextHandler.event_type(), msg)
            print(feature.get().result)


if __name__ == "__main__":
    queue = RedisConsumeQueue("queue", max_retry_interval_sec=5, max_abandon_sec=15)

    queue.register_event_handler(PrintTextHandler())

    queue.listen()

    UserInputThread(queue).start()
