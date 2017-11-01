import database
import asyncio

query = "select pg_sleep(2);"


async def run():
    await database.connect()
    futures = [database.fetch(query) for _ in range(10)]
    # Using a timeout far longer than the total expected time (which is 20 seconds)
    await asyncio.wait(futures, timeout=60)
    print("Finished")
    database.print_counts()


def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run())
    finally:
        loop.close()

if(__name__ == "__main__"):
    main()
