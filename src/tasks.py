from task import Task
from subprocess import Popen, CalledProcessError, PIPE
from tempfile import TemporaryFile


# TODO: maybe-ify out and err data
# List String -> File -> Task Error (String, String)
def subprocess(args,stdin=PIPE):
  def _subproc(rej,res):
    try:
      p = Popen(args, stdin=stdin, stdout=PIPE, stderr=PIPE, close_fds=True)
      (outdata, errdata) = p.communicate()
      ret = p.returncode
      if ret == 0:
        res((outdata,errdata))
      else:
        e = CalledProcessError(returncode=ret, cmd=" ".join(args), output=outdata)
        rej(e)
      
    except (EnvironmentError, ValueError) as e:
      rej(e)
      
  return Task(_subproc)


# String -> Task IOError File
def readfile(fname):
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


