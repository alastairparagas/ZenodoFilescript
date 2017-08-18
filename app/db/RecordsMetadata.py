import aiopg
from typing import AsyncIterable, Dict, Any
from .Entity import Entity


class RecordsMetadata(Entity):

  def __init__(self, pool: aiopg.Pool):
    super().__init__(pool)

  def get_all(self) -> AsyncIterable[Dict[str, Any]]:
    """
    Get all the rows of the records_metadata table
    :return: Generator
    """
    return self.query(
      """
      SELECT id, json, version_id, created, updated
      FROM records_metadata;
      """
    )

  async def getby_id(self, id: str) -> AsyncIterable[Dict[str, str]]:
    """
    Get a specific record from records_metadata
    :param id:
    :return:
    """
    return self.query(
      """
      SELECT id, json, version_id, created, updated
      FROM records_metadata
      WHERE id = %s
      """,
      (id,)
    )
