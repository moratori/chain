#!/usr/bin/env python
#coding:utf-8


import core
import db
import random
import time





def sleep(handler):
  print "Sleeping ..."
  db.Scoring(handler).tfidf()
  print "Waked up!"


comtable = {"sleep": sleep}

def hook(s,handler):
  head = s[0]
  if head == ":" :
    name = s[1:]
    f = comtable.get(name,None)
    if f: 
      f(handler)
      return True
    else:
      print "command not found"


def conversation(codec,length,news):
  with db.chaindb("db/chain.db") as handler:
    register = core.Regist(handler)
    generator = core.Generate(handler)
    cnt = 0

    while True:
      try:
        line = raw_input(">>> ").decode(codec)
        if (not line) or hook(line,handler) : continue
      except EOFError:
        sleep(handler)
        break

      textid = register.regist(line,False)

      reply = generator.conversation(length,textid)

      register.regist(reply,True)

      cnt += 1
      print "***",reply.encode(codec)
      if (cnt > news) : 
        sleep(handler)
        cnt = 0


def twit(codec,n):
  with db.chaindb() as handler:
    generator = core.Generate(handler)
    for d in range(n):
      print generator.random(100).encode(codec)


conversation("utf-8",45,10)
#twit("utf-8",1000)




