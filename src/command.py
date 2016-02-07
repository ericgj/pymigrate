import shlex
import csv
import re
import os.path
from StringIO import StringIO

from pymonad.Reader import curry
from pymonad.Maybe import Nothing, Just

import Task
from tasks import subprocess, tempfile, openfile, listdir, logtask, logresult_info, logresult_debug

PATTERN_SCHEMA_FILE = re.compile('(\d{14})\-do\-([\w-]*)\.sql$')
PATTERN_SCHEMA_UNDO_FILE = re.compile('(\d{14})\-undo\-*([\w-]*)\.sql$')

##### Errors

class DbError(Exception):
  def __init__(self,procerr):
    self.procerr = procerr

  def __str__(self):
    return "\n".join([ "The database returned an error:",
                       "-" * 80, 
                       self.procerr.output,
                       "-" * 80, 
                       str(self.procerr) 
                     ])

class NoDataError(Exception):
  def __str__(self):
    return "No data returned from query"

class NoValueFoundError(Exception):
  def __init__(self,field):
    self.field = field

  def __str__(self):
    return "No such field '%s' in returned data" % self.field

# Note: should never get this error
class SchemaFilenameError(Exception):
  def __init__(self,name):
    self.name = name

  def __str__(self):
    return (
      """Can't determine version and description from schema file name '%s'.
      Make sure you only have letters, numbers, hyphens, and underscores in
      the description."""
    ) % self.name


##### Commands

# Address Log -> String -> String -> Task Error (Maybe String)
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

  return logtask("Preparing database", logger, 
                 tempfile(sql) >> db_task(dbcmd)
         )

# Address Log -> String -> String -> String -> Task Error (List String)
def check(logger, dbcmd, schema_dir, table="_version_" ):
  matcher = flip(applyf)(matching_schema_files_newer_than)
  task = Task.all([
           db_current_version(logger,dbcmd,table),
           schema_current_files(logger,schema_dir)
         ]).fmap( matcher )
  msg = lambda files: "no new local schema migrations found" if len(files) == 0 else (
                        "new local schema migrations found: %d\n%s" % (
                          len(files), "\n".join([os.path.basename(f) for f in files])  )
                      )
  
  return logtask("Checking for new migration files", logger, 
           logresult_info(msg, logger, task)
         )

# Address Log -> String -> String -> String -> Task Error (List (Maybe String))
def do(logger, dbcmd, schema_dir, table="_version_" ):
  def _exec(file):
    return (
      (openfile(file)    >> 
         db_task(dbcmd)) >>
         always(db_update_for_schema_file(logger,dbcmd,file))
    )

  def _execfiles(files):
    return Task.all([_exec(f) for f in files])

  inittask  = init(logger, dbcmd, table)
  checktask = check(logger, dbcmd, schema_dir, table)
  task = (inittask >> always(checktask)) >> _execfiles

  return logtask("Running local migrations", logger, task)


# Address Log -> String -> Task Error (List String)
def schema_current_files(logger, schema_dir):
  task = listdir(schema_dir).fmap(matching_schema_files)
  msg = lambda files: (
          "no local schema migrations found" if len(files)==0 else (
            "total local schema migrations: %d\nlatest: %s" % ( 
              len(files), os.path.basename(files[-1]) )
          )
        )
  return logresult_info(msg,logger,task)


# Address Log -> String -> String -> Task Error String
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


# Note: should not have to worry about SQL injection here due to filename
# limitations.

# Address Log -> String -> String -> String -> Task Error (Maybe String)
def db_update_for_schema_file(logger, dbcmd, file, table="_version_"):
  def _task((vers,desc)):
    sql = (
      """
      INSERT INTO `%s` (version, description) VALUES ('%s','%s');
      """
    ) % (table, vers, desc)
    
    task = tempfile(sql) >> db_task(dbcmd)
    return logtask("Updating database version to %s" % vers, logger, task)

  mparts = schema_file_parts(file)
  return with_default(
    Task.reject(SchemaFilenameError(file)),
    mparts.fmap(_task)
  )


##### Underlying tasks

# String -> File -> Task Error (Maybe String)
@curry
def db_task(cmd,f):
  def _wraperr(e):
    return DbError(e)
  def _first((a,_)):
    return a

  return subprocess( shlex.split(cmd), f ).bimap( _wraperr, _first )


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


##### Helpers

def schema_file_parts(name):
  m = PATTERN_SCHEMA_FILE.search(name)
  return Nothing if m is None else Just(m.groups())

def schema_undo_file_parts(name):
  m = PATTERN_SCHEMA_UNDO_FILE.search(name)
  return Nothing if m is None else Just(m.groups())
  

def matching_schema_files(files):
  def _filter(name):
    return True if PATTERN_SCHEMA_FILE.search(name) else False
  return sorted(filter(_filter,files))

def matching_schema_undo_files(files):
  def _filter(name):
    return True if PATTERN_SCHEMA_UNDO_FILE.search(name) else False
  return sorted(filter(_filter,files))

@curry
def matching_schema_files_newer_than(version,files):
  def _filter(name):
    match = PATTERN_SCHEMA_FILE.search(name)
    if not match:
      return False
    (ts,_) = match.groups()
    return True if ts > version else False
  return sorted(filter(_filter,files))



# TODO move these elsewhere

def always(x):
  return lambda _: x

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

