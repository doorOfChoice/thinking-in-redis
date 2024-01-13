from queue.redis_queue import RedisQueue, IRedisEventHandler, RedisEvent


class PrintTextHandler(IRedisEventHandler):
    def handle(self, event: RedisEvent):
        print(event)

    @classmethod
    def event_type(cls) -> str:
        return "text"


if __name__ == "__main__":
    queue = RedisQueue("queue")

    queue.register_event_handler(PrintTextHandler())
