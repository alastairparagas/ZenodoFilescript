import aiopg
from typing import AsyncIterable, Dict, Any


class Entity:

  def __init__(self, pool: aiopg.Pool):
    self.pool = pool

  async def query(
      self,
      query_string: str,
      query_tuple: tuple = tuple()
  ) -> AsyncIterable[Dict[str, Any]]:
    """
    Custom query for the records_metadata table
    :param query_string
    :param query_tuple
    :return: Generator
    """
    connection = await self.pool.acquire()

    cursor = await connection.cursor()
    await cursor.execute(query_string, query_tuple)

    current_row = await cursor.fetchone()
    while current_row is not None:
      column_name_and_values = zip(
        map(lambda x: x[0], cursor.description),
        current_row
      )
      yield {
        column_name: value
        for (column_name, value) in column_name_and_values
      }

      current_row = await cursor.fetchone()

    self.pool.release(connection)
