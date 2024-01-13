from threading import Thread

from example_redis_queue.r_queue import IRedisEventHandler, RedisEvent, RedisQueue


class PrintTextHandler(IRedisEventHandler):
    def handle(self, event: RedisEvent):
        print(event)

    @classmethod
    def event_type(cls) -> str:
        return "text"


class UserInputThread(Thread):
    def __init__(self, queue: RedisQueue):
        super().__init__()
        self.queue = queue

    def run(self) -> None:
        while True:
            msg = input("input your msg:")
            for i in range(0, 10):
                queue.push_event(PrintTextHandler.event_type(), msg)


if __name__ == "__main__":
    queue = RedisQueue("queue")

    queue.register_event_handler(PrintTextHandler())

    queue.listen()

    UserInputThread(queue).start()
