import aiopg

async def connect(username: str, password: str,
                  dbname: str, host: str="127.0.0.1", port: int=5432) -> aiopg.Pool:
  """
  Attempts to connect to Postgres and creates a connection pool
  :param username:
  :param password:
  :param dbname:
  :param host:
  :param port:
  :return: aiopg.Pool
  """
  pool = await aiopg.create_pool(
    'dbname={} user={} password={} host={} port={}'.format(
      dbname, username, password, host, port
    )
  )

  return pool
