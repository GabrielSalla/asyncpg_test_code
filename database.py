import asyncio
import logging
import asyncpg
import traceback
import decimal

_pool = None


async def connect():
    # Create a connection pool for each database defined in the configuration
    global _pool
    _pool = await asyncpg.create_pool(
        host="localhost",
        user="postgres",
        password="postgres",
        database="postgres",
        port=5432,
        min_size=0,
        max_size=3,
        max_queries=30,
        timeout=5,
        command_timeout=10,
        max_inactive_connection_lifetime=180
    )


async def close_connections():
    await _pool.close()


async def _fetch_data( query, *args):
    # Acquire a connection
    async with _pool.acquire(timeout=30) as connection:
        try:
            result = await connection.fetch(query, *args, timeout=3)
            print(".")
            return [dict(row) for row in result]
        except asyncio.TimeoutError:
            print(f"Query timed out\n{query}")


async def fetch(query, *args):
    try:
        return await _fetch_data(query, *args)
    except:
        print(f"{traceback.format_exc()}\n{query} {args}")
        return None


def print_counts():
    print("Queue size =", _pool._queue.qsize())