import shlex
import csv
import re
from StringIO import StringIO

from pymonad.Reader import curry
from pymonad.Maybe import Nothing, Just

import Task
from tasks import subprocess, tempfile, listdir, logtask, logresult_info, logresult_debug

PATTERN_SCHEMA_FILE = re.compile('^(\d{14})\-do\-([\w-]*)\.sql$')
PATTERN_SCHEMA_UNDO_FILE = re.compile('^(\d{14})\-undo\-*([\w-]*)\.sql$')

class NoDataError(Exception):
  def __str__(self):
    return "NoDataError: No data returned from query"

class NoValueFoundError(Exception):
  def __init__(self,field):
    self.field = field

  def __str__(self):
    return "NoValueFoundError: No such field '%s' in returned data" % self.field


def init(logger, dbcmd, table="_version_" ):
  
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

  return logtask("Preparing database, if needed", logger, 
                 tempfile(sql) >> db_task(dbcmd)
         )


def check(logger, dbcmd, schema_dir, table="_version_" ):
  matcher = flip(applyf)(matching_schema_files_newer_than)
  task = Task.all([
           db_current_version(logger,dbcmd,table),
           schema_current_files(logger,schema_dir)
         ]).fmap( matcher )
  msg = lambda files: "no new local schema migrations found" if len(files) == 0 else (
                      "new local schema migrations found: %s" % ", ".join(files) )
  
  return logtask("Checking for new migration files", logger, 
           logresult_info(msg, logger, task)
         )


def schema_current_files(logger, schema_dir):
  task = listdir(schema_dir).fmap(matching_schema_files)
  msg = lambda files: (
          "total local schema migrations: %d (latest: %s)" % ( len(files), files[-1] )
        )
  return logresult_info(msg,logger,task)


# String -> String -> Task Error String
def db_current_version(logger, dbcmd, table="_version_" ):
  
  sql = (
    """
    SELECT version FROM `%s` WHERE id = (SELECT MAX(id) FROM `%s`);
    """
  ) % (table, table)

  task = (tempfile(sql) >> db_getvalue(dbcmd,'version')) >> (
           reject_unless(NoValueFoundError('version'))
         )
  msg = lambda v: "database version: %s" % v
  return logresult_info(msg, logger, task)


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


def matching_schema_files(files):
  def _filter(name):
    return True if PATTERN_SCHEMA_FILE.match(name) else False
  return sorted(filter(_filter,files))

def matching_schema_undo_files(files):
  def _filter(name):
    return True if PATTERN_SCHEMA_UNDO_FILE.match(name) else False
  return sorted(filter(_filter,files))

@curry
def matching_schema_files_newer_than(version,files):
  def _filter(name):
    match = PATTERN_SCHEMA_FILE.match(name)
    if not match:
      return False
    (ts,_) = match.groups()
    return True if ts > version else False
  return sorted(filter(_filter,files))



# TODO move these elsewhere

def flip(fn):
  def _fn(a,b,*args,**kwargs):
    return fn(b,a,*args,**kwargs)
  return curry(_fn)

@curry
def applyf(args,fn):
  return fn(*args)

@curry
def with_default(val,maybe):
  return val if maybe == Nothing else maybe.getValue()

@curry
def reject_unless(val,maybe):
  return with_default(
    Task.reject(val),
    maybe.fmap( lambda x: Task.resolve(x) )
  )

