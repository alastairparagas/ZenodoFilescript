import aiopg
from .Entity import Entity


class FulltextKeyword(Entity):

  def __init__(self, pool: aiopg.Pool):
    super().__init__(pool)

  async def insert_keyword(self, keyword) -> None:
    """
    Get all the rows of the records_metadata table
    :return: Generator
    """
    connection = await self.pool.acquire()

    try:
      cursor = await connection.cursor()
      await cursor.execute(
        "SELECT create_keyword(%s)",
        (keyword,)
      )
    finally:
      self.pool.release(connection)
