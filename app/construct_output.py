from typing import AsyncIterable, Union
import sys
from aiopg import Pool
import aitertools
import psycopg2
from app.db import RecordsMetadata, FulltextKeyword

async def construct_output(
    source_db_pool: Pool,
    fulltext_db_pool: Union[Pool, None],
    file_handle,
    load_on_db=False
) -> None:
  """
  :param source_db_pool:
  :param fulltext_db_pool:
  :param file_handle:
  :param load_on_db:
  :return:
  """
  records_metadata = RecordsMetadata.RecordsMetadata(source_db_pool)

  try:
    if load_on_db:
      iterator1, iterator2 = aitertools.tee(
        source_transformer_db(records_metadata),
        2
      )
      await sink_file(iterator1, file_handle)
      if fulltext_db_pool is not None:
        await sink_fulltext_db(iterator2, fulltext_db_pool)
    else:
      iterator1, iterator2 = aitertools.tee(
        source_transformer_db(records_metadata),
        2
      )
      await sink_file(iterator1, file_handle)
      if fulltext_db_pool is not None:
        await sink_fulltext_db(iterator2, fulltext_db_pool)
  except (psycopg2.Error, IOError) as e:
    print('Error in the middle of constructing output: {}'.format(e))
    await file_handle.close()
    source_db_pool.terminate()
    await source_db_pool.wait_closed()
    if fulltext_db_pool is not None:
      fulltext_db_pool.terminate()
      await fulltext_db_pool.wait_closed()
    sys.exit(-1)
  finally:
    await file_handle.close()
    source_db_pool.terminate()
    await source_db_pool.wait_closed()
    if fulltext_db_pool is not None:
      fulltext_db_pool.terminate()
      await fulltext_db_pool.wait_closed()

async def source_transformer_db(
    records_metadata: RecordsMetadata
) -> AsyncIterable[str]:
  """
  Transforms and filters through raw DB records and creates unique
  keyword vertex determination on-database
  :param records_metadata:
  :return: AsyncIterable[str]
  """
  source = records_metadata.get_all()
  async for record in source:
    record_metadata = record.get('json')
    if record_metadata is None:
      continue
    if (record_metadata.get('resource_type', {}).get('type') != 'publication' or
        record_metadata.get('resource_type', {}).get('subtype') != 'article'):
      continue
    keywords = record_metadata.get('keywords', [])
    if len(keywords) == 0:
      continue

    documentid_vertex = '{},{},{}'.format(
      record.get('id'),
      record_metadata.get('_oai', {}).get('id', ''),
      'document'
    )

    def keyword_generator():
      atomic_keywords = [atomic_keyword
                         for keyword in keywords
                         for atomic_keyword in keyword.split(',')]
      for atomic_keyword in atomic_keywords:
        normalized_atomic_keyword = atomic_keyword\
          .replace('"', r'\"')\
          .replace("\r", '') \
          .replace("\n", ' ')\
          .strip()

        if normalized_atomic_keyword.replace(' ', '') is '':
          continue
        yield normalized_atomic_keyword
    to_keyword_edges = ','.join(keyword_generator())

    yield '{},{}'.format(documentid_vertex, to_keyword_edges)

  source = records_metadata.query(
    """
    SELECT DISTINCT trim(
      replace(replace(replace(replace(replace(replace(
        trim(
          both ',' from regexp_split_to_table(
            json_array_elements_text(json_extract_path(json, 'keywords')),
            ','
          )
        ),
      CHR(10), ''),
      CHR(13), ''),
      CHR(7), ''),
      CHR(8), ''),
      CHR(9), ''),
      CHR(160), '')
    ) as atomic_keywords
    FROM records_metadata;
    """
  )
  async for record in source:
    atomic_keyword = record.get('atomic_keywords')\
      .replace('"', r'\"')\
      .replace("\n", ' ')\
      .strip()

    if atomic_keyword.replace(' ', '') is '':
      continue
    yield '{}, ,{}'.format(atomic_keyword, 'keyword')

async def source_transformer_local(
    records_metadata: RecordsMetadata
) -> AsyncIterable[str]:
  """
  Transforms and filters through raw DB records and creates unique
  keyword vertex determination on-script
  :param records_metadata:
  :param source: AsyncIterable yielding DB records
  :return: AsyncIterable[str]
  """
  source = records_metadata.get_all()
  global_keywords_set = set()

  async for record in source:
    record_metadata = record.get('json')
    if record_metadata is None:
      continue
    if (record_metadata.get('resource_type', {}).get('type') != 'publication' or
        record_metadata.get('resource_type', {}).get('subtype') != 'article'):
      continue
    keywords = record_metadata.get('keywords', [])
    if len(keywords) == 0:
      continue

    documentid_vertex = '{},{},{}'.format(
      record.get('id'),
      record_metadata.get('_oai', {}).get('id', ''),
      'document'
    )

    def keyword_generator():
      atomic_keywords = [atomic_keyword
                         for keyword in keywords
                         for atomic_keyword in keyword.split(',')]
      for atomic_keyword in atomic_keywords:
        normalized_atomic_keyword = atomic_keyword\
          .replace('"', r'\"')\
          .replace("\r", '') \
          .replace("\n", ' ')\
          .strip()

        if normalized_atomic_keyword.replace(' ', '') is '':
          continue
        yield normalized_atomic_keyword

    document_keywords_set = set(keyword_generator())
    global_keywords_set = global_keywords_set.union(document_keywords_set)
    to_keywords_edges = ','.join(document_keywords_set)

    yield '{},{}'.format(documentid_vertex, to_keywords_edges)

  for keyword in global_keywords_set:
    yield '{}, ,{}'.format(keyword, 'keyword')

async def sink_file(
    source: AsyncIterable[str], output_file
) -> None:
  """
  Stores individual yielded elements as individual lines
  :param source:
  :param output_file:
  :return:
  """
  async for data in source:
    print(data)
    await output_file.write(data + '\n')

async def sink_fulltext_db(
    source: AsyncIterable[str], db_pool: Pool
) -> None:
  """
  Stores yielded keyword elements into the fulltext DB
  :param source:
  :param db_pool:
  :return:
  """
  fulltext_keyword = FulltextKeyword.FulltextKeyword(db_pool)

  async for data in source:
    line_split = data.split(',')
    vertex_type = line_split[2]
    vertex_id = line_split[0]

    if vertex_type == 'keyword':
      print(vertex_id)
      await fulltext_keyword.insert_keyword(vertex_id)
