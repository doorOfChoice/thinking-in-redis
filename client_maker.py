import redis

_redis_client = redis.StrictRedis(
    decode_responses=True, db=10, host="localhost", port=6379
)


def get_redis_client():
    return _redis_client
