from datetime import datetime
from threading import Thread

from redis_queue.delay_consume_queue import DelayConsumeQueue


class UserInputThread(Thread):
    """
    用户录入消息的线程
    """

    def __init__(self, queue: DelayConsumeQueue):
        super().__init__()
        self.queue = queue

    def run(self) -> None:
        while True:
            msg = input("input your msg:")
            now = int(datetime.now().timestamp()) + 5
            self.queue.push_event("text", msg, now)


if __name__ == "__main__":
    queue = DelayConsumeQueue("delay")

    queue.listen()

    UserInputThread(queue).start()