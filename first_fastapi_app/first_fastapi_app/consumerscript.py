# from aiokafka import AIOKafkaConsumer
# import asyncio

# async def consume():
#     consumer = AIOKafkaConsumer(
#         'my_todos',
#         bootstrap_servers='broker:19092',
#         group_id='my-group',
#         auto_offset_reset='earliest'
#     )
#     await consumer.start()
#     try:
#         async for msg in consumer:
#             print("Consumed: ", msg.value.decode())
#     finally:
#         await consumer.stop()

# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(consume())

from aiokafka import AIOKafkaConsumer
import asyncio

async def consume():
    consumer = AIOKafkaConsumer(
        'my_script_01',
        bootstrap_servers='localhost:9092',  # Pointing to local broker
        group_id='my-script-group_01',
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print("Consumed: ", msg.value.decode())
    finally:
        await consumer.stop()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()  # Use a new event loop
    asyncio.set_event_loop(loop)
    loop.run_until_complete(consume())
