import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis(
        'redis://localhost')

    id_1 = await redis.xadd('mystream', {'hello': b'world'})
    id_2 = await redis.xadd('mystream', {'hello': b'world'})
    print(id_1, id_2)

    value_from_range = await redis.xrange('mystream', count=2)
    print(value_from_range)

    value_read = await redis.xread('mystream', start=id_1)
    print(value_read)

    redis.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
