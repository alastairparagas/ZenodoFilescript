from app.db import utils
import app.user_input as user_input
from app.construct_output import construct_output
import asyncio
import aiofiles
import psycopg2
import sys

async def main() -> None:
  try:
    source_db_credentials, fulltext_db_credentials, \
    output_file_name, load_on_db = await user_input.acquire_arguments()
  except user_input.UnsatisfactoryArguments:
    source_db_credentials = await user_input.source_db_credentials()
    fulltext_db_credentials = await user_input.fulltext_db_credentials()
    output_file_name = await user_input.output_file()
    load_on_db = await user_input.load_on_db()

  try:
    source_db_pool = await utils.connect(**source_db_credentials)
    fulltext_db_pool = None
    if fulltext_db_credentials is not None:
      fulltext_db_pool = await utils.connect(**fulltext_db_credentials)
    file_handle = await aiofiles.open(output_file_name, mode='w+')
  except psycopg2.Error as e:
    print('Error connecting to the database: {}'.format(e))
    sys.exit(-1)
  except IOError:
    print('Error trying to construct output file')
    sys.exit(-1)

  await construct_output(
    source_db_pool=source_db_pool,
    fulltext_db_pool=fulltext_db_pool,
    file_handle=file_handle,
    load_on_db=load_on_db
  )

if __name__ == '__main__':
  asyncio.get_event_loop().run_until_complete(main())
