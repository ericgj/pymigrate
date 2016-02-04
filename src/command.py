import shlex
import csv
from StringIO import StringIO

from pymonad.Reader import curry
from pymonad.Maybe import Nothing, Just

import Task
from tasks import subprocess, tempfile

class NoDataError(Exception):
  def __str__(self):
    return "NoDataError: No data returned from command"


def init(dbcmd, table="_version_" ):
  
  sql = (
    """
    CREATE TABLE IF NOT EXISTS `%s` (
      id INT(11) AUTO_INCREMENT PRIMARY KEY,
      version CHAR(14),
      description TEXT NULL,
      commit CHAR(40)
    );
    """
  ) % (table)

  return tempfile(sql) >> db_task(dbcmd)


def check(dbcmd, schema_dir, table="_version_" ):
  pass


def db_current_version(dbcmd, table="_version_" ):
  
  sql = (
    """
    SELECT version FROM (
      SELECT id, version FROM `%s` GROUP BY id HAVING id = MAX(id)
    ) AS last;
    """
  ) % (table)

  return tempfile(sql) >> db_getvalue(dbcmd,'version')


# String -> File -> Task Error (Maybe String)
@curry
def db_task(cmd,f):
  return subprocess( shlex.split(cmd), f ).fmap( lambda (out,_): out )

# String -> File -> Task Error (Maybe DictReader)
@curry
def db_select(cmd,f):
  def _reader(mdata):
    return mdata.fmap( 
      lambda data: csv.DictReader(StringIO(data), delimiter="\t") 
    )
  return db_task(cmd,f).fmap(_reader)

# String -> String -> File -> Task Error (Maybe String)
@curry
def db_getvalue(cmd,field,f):
  return (db_select(cmd,f) >> reject_unless(NoDataError())).fmap(
    reader_getvalue(field)
  )

# String -> DictReader -> Maybe String
@curry
def reader_getvalue(field,reader):
  try:
    row = reader.next()
    return Just(row[field])
  except (StopIteration, KeyError):
    return Nothing



# TODO move these elsewhere

@curry
def with_default(val,maybe):
  return val if maybe == Nothing else maybe.getValue()

@curry
def reject_unless(val,maybe):
  return with_default(
    Task.reject(val),
    maybe.fmap( lambda x: Task.resolve(x) )
  )

