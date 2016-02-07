from subprocess import Popen, CalledProcessError, PIPE
from tempfile import TemporaryFile
from logging import DEBUG, INFO, WARN, ERROR
import os

from pymonad.Maybe import Nothing, Just
from task import Task
import log

# List String -> File -> Task Error (Maybe String, Maybe String)
def subprocess(args,stdin=PIPE):
  def _subproc(rej,res):
    try:
      p = Popen(args, stdin=stdin, stdout=PIPE, stderr=PIPE, close_fds=True)
      (outdata, errdata) = p.communicate()
      ret = p.returncode
      mout = Nothing if len(outdata)==0 else Just(outdata)
      merr = Nothing if len(errdata)==0 else Just(errdata)
      if ret == 0:
        res((mout,merr))
      else:
        e = CalledProcessError(returncode=ret, cmd=" ".join(args), output=outdata)
        rej(e)
      
    except (EnvironmentError, ValueError) as e:
      rej(e)
      
  return Task(_subproc)


# String -> Task IOError File
def openfile(fname):
  def _read(rej,res):
    try:
      res( open(fname) )

    except IOError as e:
      rej(e)

  return Task(_read)


# String -> Task IOError File
def tempfile(str):
  def _read(rej,res):
    try:
      f = TemporaryFile()
      f.write(str)
      f.flush()
      f.seek(0)
      res( f )

    except IOError as e:
      rej(e)

  return Task(_read)


def listdir(path):
  def _list(rej,res):
    try:
      res( os.listdir(path) )
    
    except OSError as e:
      rej(e)

  return Task(_list)


def logtask(msg,addr,task):
  def _start(rej,res):
    try:
      log.start(msg,addr)
      res(None)
    except Exception as e:
      rej(e) 

  def _end(rej,res):
    try:
      log.end(msg,addr)
      res(None)
    except Exception as e:
      rej(e)

  return (Task(_start) >> task) >> Task(_end)


def logresult(fn,addr,lvl,task):
  def _log(lvl):
    def _tap(x):
      try:
        log.log(lvl,fn(x),addr)
        return x
      except Exception:
        return x
    return _tap

  return task.bimap( _log(ERROR), _log(lvl) )

def logresult_debug(fn,addr,task):
  return logresult(fn,addr,DEBUG,task)

def logresult_info(fn,addr,task):
  return logresult(fn,addr,INFO,task)

