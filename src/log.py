
from logging import DEBUG, INFO, WARN, ERROR
from collections import namedtuple

Log = namedtuple('Log', ['level','message','initial','final'])

def log(lvl,msg,addr,initial=False,final=False):
  addr( Log(lvl,msg,initial,final) )

def start(msg,addr):
  log(INFO,msg,addr,initial=True)

def end(msg,addr):
  log(INFO,msg,addr,final=True)

def debug(msg,addr):
  log(DEBUG,msg,addr)

def info(msg,addr):
  log(INFO,msg,addr)

def warn(msg,addr):
  log(WARN,msg,addr)

def error(msg,addr):
  log(ERROR,msg,addr)


