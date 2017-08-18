import prompt_toolkit.validation
import prompt_toolkit
from typing import Dict, Tuple, Union
import os
from argparse import ArgumentParser


class UnsatisfactoryArguments(BaseException):
  pass

async def acquire_arguments() -> Tuple[Dict[str, str],
                                       Union[Dict[str, str], None],
                                       str,
                                       bool]:
  """
  Check if program arguments were passed where we can infer DB credentials,
  output file name and whether or not uniqueness computation should be done
  on the process execution computer or the db server computer
  :return: (
    source db credentials :dict:,
    fulltext db credentials :dict:
    file name :str:,
    load on db :bool:
  )
  """
  parser = ArgumentParser()
  parser.add_argument('--source_db_username', help='DB username')
  parser.add_argument('--source_db_password', help='DB password')
  parser.add_argument('--source_db_name', help='DB name')
  parser.add_argument('--source_db_host', help='DB host')
  parser.add_argument('--source_db_port', help='DB port', type=int)
  parser.add_argument('--create_fulltext_db', help='Should a fulltext search DB be '
                                                   'created? (Bool)', type=bool, default=False)
  parser.add_argument('--fulltext_db_username', help='DB username')
  parser.add_argument('--fulltext_db_password', help='DB password')
  parser.add_argument('--fulltext_db_name', help='DB name')
  parser.add_argument('--fulltext_db_host', help='DB host')
  parser.add_argument('--fulltext_db_port', help='DB port', type=int)
  parser.add_argument('--file_name', help='Output file name with '
                                          'relative/absolute path')
  parser.add_argument('--load_on_db', help='Should most of the computation '
                                           'load be on db? (Bool)', type=bool, default=False)

  arguments = parser.parse_args()
  for required_attr in ['source_db_username',
                        'source_db_password',
                        'source_db_name',
                        'file_name']:
    if getattr(arguments, required_attr) is None:
      raise UnsatisfactoryArguments()

  source_db_credentials = {
    'username': arguments.source_db_username,
    'password': arguments.source_db_password,
    'dbname': arguments.source_db_name
  }
  if hasattr(arguments, 'source_db_host') and arguments.source_db_host is not None:
    source_db_credentials['host'] = arguments.source_db_host
  if hasattr(arguments, 'source_db_port') and arguments.source_db_port is not None:
    source_db_credentials['port'] = arguments.source_db_port

  fulltext_db_credentials = None
  if arguments.create_fulltext_db is True:
    for required_attr in ['fulltext_db_username',
                          'fulltext_db_password',
                          'fulltext_db_name']:
      if getattr(arguments, required_attr) is None:
        raise UnsatisfactoryArguments()

      fulltext_db_credentials = {
        'username': arguments.fulltext_db_username,
        'password': arguments.fulltext_db_password,
        'dbname': arguments.fulltext_db_name,
      }
      if hasattr(arguments, 'fulltext_db_host') and arguments.fulltext_db_host is not None:
        fulltext_db_credentials['host'] = arguments.fulltext_db_host
      if hasattr(arguments, 'fulltext_db_port') and arguments.fulltext_db_port is not None:
        fulltext_db_credentials['port'] = arguments.fulltext_db_port

  return source_db_credentials, fulltext_db_credentials, \
      str(arguments.file_name), arguments.load_on_db


class BlankSpaceValidator(prompt_toolkit.validation.Validator):
  def validate(self, document):
    if document.text == '':
      raise prompt_toolkit.validation.ValidationError(
        message='Blank space was entered',
        cursor_position=len(document.text)
      )


class NumericValidator(prompt_toolkit.validation.Validator):
  def validate(self, document):
    if document.text:
      try:
        int(document.text)
      except ValueError:
        raise prompt_toolkit.validation.ValidationError(
          message='Integer value required',
          cursor_position=len(document.text)
        )


class BooleanValidator(prompt_toolkit.validation.Validator):
  def validate(self, document):
    if document.text:
      try:
        if document.text not in ['True', 'False']:
          raise ValueError
        bool(document.text)
      except ValueError:
        raise prompt_toolkit.validation.ValidationError(
          message='Boolean value required',
          cursor_position=len(document.text)
        )

async def source_db_credentials() -> Dict[str, str]:
  """
  Prompts the user for his/her DB credentials
  :return: Map of user-entered DB credentials
  """
  db_username = await prompt_toolkit.prompt_async(
    'Source DB Username: ',
    validator=BlankSpaceValidator()
  )
  db_password = await prompt_toolkit.prompt_async(
    'Source DB Password: ',
    is_password=True,
    validator=BlankSpaceValidator()
  )
  db_name = await prompt_toolkit.prompt_async(
    'Source DB Name: ',
    validator=BlankSpaceValidator()
  )
  db_host = await prompt_toolkit.prompt_async(
    'Source DB Host: '
  )
  db_port = await prompt_toolkit.prompt_async(
    'Source DB Port: ',
    validator=NumericValidator()
  )

  credentials = {
    'username': db_username,
    'password': db_password,
    'dbname': db_name
  }

  if db_host != '':
    credentials['host'] = db_host
  if db_port != '':
    credentials['port'] = db_port

  return credentials

async def fulltext_db_credentials() -> Union[Dict[str, str], None]:
  """
  Fulltext DB Credentials
  :return: Optional Map of the user-entered fulltext DB credentials
  """
  create_fulltext_db = await prompt_toolkit.prompt_async(
    'Create Fulltext DB (True/False): ',
    validator=BooleanValidator()
  )

  if bool(create_fulltext_db) is False:
    return None

  db_username = await prompt_toolkit.prompt_async(
    'Fulltext DB Username: ',
    validator=BlankSpaceValidator()
  )
  db_password = await prompt_toolkit.prompt_async(
    'Fulltext DB Password: ',
    is_password=True,
    validator=BlankSpaceValidator()
  )
  db_name = await prompt_toolkit.prompt_async(
    'Fulltext DB Name: ',
    validator=BlankSpaceValidator()
  )
  db_host = await prompt_toolkit.prompt_async(
    'Fulltext DB Host: '
  )
  db_port = await prompt_toolkit.prompt_async(
    'Fulltext DB Port: ',
    validator=NumericValidator()
  )

  credentials = {
    'username': db_username,
    'password': db_password,
    'dbname': db_name
  }

  if db_host != '':
    credentials['host'] = db_host
  if db_port != '':
    credentials['port'] = db_port

  return credentials

async def output_file() -> str:
  """
  Asks the user where to store the output file
  :return: File name where to store the output
  """
  file_name = await prompt_toolkit.prompt_async(
    'File name: ',
    validator=BlankSpaceValidator()
  )

  return os.path.abspath(file_name)

async def load_on_db() -> bool:
  """
  Asks the user if the load should mostly be on
  db server or server where this script is running
  :return: bool
  """
  load_on_db_bool = await prompt_toolkit.prompt_async(
    'Load on DB (True/False): ',
    validator=BooleanValidator()
  )

  return bool(load_on_db_bool)
