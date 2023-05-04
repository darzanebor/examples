#!/usr/bin/python3
import asyncio

from aiobotocore.session import get_session
from aiobotocore.config import AioConfig

from os import environ as env
from aiohttp import web

app = web.Application()


async def sqs_process_messages():
    """init SQS client"""
    session = get_session()
    async with session.create_client(
        service_name="sqs",
        endpoint_url=env.get("IOSHI_SQS_ENDPOINT"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        region_name="ru-central1",
        config=AioConfig(
            connect_timeout=45,
            read_timeout=90,
            retries={"max_attempts": 10},
        ),
    ) as client:
        queue_url = env.get("IOSHI_SQS_QUEUE")
        while True:
            try:
                response = await client.receive_message(
                    QueueUrl=queue_url,
                )
                if "Messages" in response:
                    for msg in response["Messages"]:
                        print(f'Got msg "{msg["Body"]}"')
                        await client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                else:
                    print("No messages in queue")
                await asyncio.sleep(10)
            except Exception as e:
                print(e)


async def main():
    await asyncio.gather(
        sqs_process_messages(),
        web._run_app(
            app,
            host=env.get("IOSHI_LISTEN_HOST", "0.0.0.0"),
            port=int(env.get("IOSHI_LISTEN_PORT", "5000")),
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
