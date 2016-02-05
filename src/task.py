
from pymonad.Monad import *
import pymonad.Either
import pymonad.Maybe
from pymonad.Reader import curry

class Task(Monad):

  def __init__(self, computation):
    self.fork = computation

  def __mult__(self,fn):
    return self.fmap(fn)

  def fmap(self, fn):
    def _mapfn(rej,res):
      return self.fork( lambda a: rej(a),  lambda b: res(fn(b)) )
    return Task(_mapfn) 
  
  def bind(self, fn):
    def _bindfn(rej,res):
      return self.fork( lambda a: rej(a),  lambda b: fn(b).fork(rej,res) )
    return Task(_bindfn)
  
  def amap(self, fvalue):
    return self.bind( lambda f: fvalue.fmap(f) ) 
  
  def bimap(self,rejfn,resfn):
    def _bimapfn(rej,res):
      return self.fork( lambda a: rej(rejfn(a)), lambda b: res(resfn(b)) )
    return Task(_bimapfn)

  @classmethod
  def unit(cls, value):
    return Task( lambda _,res: res(value) )


def reject(val):
  return Task( lambda rej,_: rej(val) )

resolve = Task.unit


def to_either(task):
  return task.bimap( Either.Left, Either.Right )

def to_maybe(task):
  return task.bimap( lambda _: Maybe.Nothing, Maybe.Just )

    
# TODO make parallel / immutable
# Python's scope rules make this a PITA

def all(tasks):
  def _alltask(rej,res):
    runstate = {
      'len': len(tasks),
      'result': [None]*len(tasks),
      'resolved': False
    }

    @curry
    def _rej(state,e):
      if state['resolved']:
        return None
      state['resolved'] = True
      rej(e)

    @curry
    def _res(state,x):
      if state['resolved']:
        return None
      state['result'][i] = x
      state['len'] = state['len'] - 1
      if state['len'] == 0:
        state['resolved'] = True
        res(state['result'])

    def _run(state,i,t):
      return t.fork(_rej(state), _res(state))

    if len(tasks) == 0:
      res([])
    else:
      for i,task in enumerate(tasks):
        _run(runstate,i,task)

  return Task(_alltask)

