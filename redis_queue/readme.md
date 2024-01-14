# 消息队列

消息队列是一种非常常用的中间件，主要用于大量任务处理时候的削峰。

如果没有消息队列，当100W个请求同时来临时，这些服务器不得不被迫同时处理100W个请求，如果单个请求本身处理就比较慢，那么就会导致服务器连接数打满假死，无法继续处理数据，是非常严重的事故。

# 如何实现用Redis

消息队列中，会有两个对象, `生产者` 和 `消费者`。

生产者在字面意思上push数据到队列， 消费者字面意思上从队列里面获取数据处理。

那么在redis里面就会有两对命令非常适合做这件事:
1. `lpush` and `brpop` (lpush负责往队列的左边push数据, rbpop负责不断的从队列右边获取数据，是一个FIFO结构)
2. `rpush` and `blpop` (rpush负责往队列的右边push数据, lbpop负责不断的从队列左边获取数据，是一个FIFO结构)

所以一个最简单的demo就是

```python
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
ConsumerThread().start()
ProducerThread().start()
```
[see demo1](example_simple_demo.py)

所以，一个简单的消息队列就这样被实现了。

## 消息没有处理就宕机?

但是这个之中有一个非常大的不足，当然有很多不足啦，比如单个服务器一次只能处理一条消息balabala，但是我感觉这不是重点。

重要的是，当consumer收到消息之后，有可能消息还没来得及处理，服务就重启，或者处理中出现了异常，这些行为都可能导致数据丢失。

这里会用到两个非常有用的命令
1. `brpoplpush a_list b_list` - 把a_list最右边的元素加到b最左边
2. `lrem a_list count element` - 从a_list里面移除值为element的元素

[rbpoplpush官方文档](https://redis.io/commands/rpoplpush/)这样描述这个命令:
> Redis is often used as a messaging server to implement processing of background jobs or other kinds of messaging tasks. A simple form of queue is often obtained pushing values into a list in the producer side, and waiting for this values in the consumer side using RPOP (using polling), or BRPOP if the client is better served by a blocking operation.<br/>
> However in this context the obtained queue is not reliable as messages can be lost, for example in the case there is a network problem or if the consumer crashes just after the message is received but before it can be processed.<br/>
> RPOPLPUSH (or BRPOPLPUSH for the blocking variant) offers a way to avoid this problem: the consumer fetches the message and at the same time pushes it into a processing list. It will use the LREM command in order to remove the message from the processing list once the message has been processed.<br/>
> An additional client may monitor the processing list for items that remain there for too much time, pushing timed out items into the queue again if needed.<br/>

总的来说通过brpoplpush可以达到两段确认的效果：
1. 有一个任务队列task_queue
2. 有一个正在处理的任务队列process_queue
3. 通过`brpoplpush task_queue process_queue` 从task_queue取出数据，并且lpush到process_queue, 这个适合数据到了正在处理的任务队列
4. 处理数据
5. 处理完数据从正在处理的任务队列process_queue中`lrem process_queue 0 data`移除掉数据，这样数据就完全被消费掉了。

这里可以看到lrem是移除列表中和data相等的元素，那么为了保证消息的唯一性，可以对push到redis队列里面的元素规定一个固定的格式，并且包含唯一键，比如:
```json
{
  "event_type": "order",
  "event_id": "uuid",
  "created_time": 17890009090,
  "content": "消息内容"
}
```

那么如果出现我们刚刚说的，消息没有处理的情况，那么消息就会一直留在process_queue中.

## process_queue中未处理的消息，如何重新执行

新开一个线程，定时去扫描process_queue中的元素，按照上文的设计，每个消息都有创建时间，用当前时间减去创建时间得到一个时间差。
1. 如果时间差在可重试阈值范围，就重放进task_queue;
2. 如果时间差在丢弃阈值范围，就直接从process_data丢弃掉。


[点击这里看可靠消息队列的实现](consume_queue.py)
[点击这里看如何使用](example_simple_demo.py)
