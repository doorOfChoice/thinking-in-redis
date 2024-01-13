import redis

"""
请在这里修改你的redis地址
"""
__redis_client = redis.StrictRedis(
    decode_responses=True, db=10, host="localhost", port=6379
)


def get_redis_client():
    return __redis_client
