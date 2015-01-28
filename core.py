#!/usr/bin/env python
#coding:utf-8

import db
import random
import pickle
import MeCab
import os
import math

random.seed()


def collect(text,pred = lambda x,y: True):
  """
    textを形態素解析しpredが真であると判定した語だけを
    (語 , 品詞)のリストのunicode型で返す.
    predはunicode型の語とsplitされたfeatureのリストをとる2引数関数

    textは Python の unicode型を意図している
  """
  result = []
  encoded = text.encode("utf-8")
  # MeCabに渡せるのは utf-8
  tagger = MeCab.Tagger("-Ochasen")
  node = tagger.parseToNode(encoded)
  while node:
    feature = map(lambda x: x.decode("utf-8") , node.feature.split(","))
    if node.surface and pred(node.surface,feature):
      result.append((node.surface.decode("utf-8"),feature[0]))
    node = node.next
  return result



class Regist:

  def __init__(self,handler):
    self.handler = handler
    
  def __regist_termlevel(self,termsid):
    """
      各語の前後の関係とその頻度をrelmapper,relweightに
      先頭の形態素の関係をstartnextに
      文の最後の形態素の出現をlasttermに
      登録する

    """
    for i,e in enumerate(termsid):
      if i < len(termsid)-1:
        (tid1,tid2) = (e,termsid[i+1])
        rid = self.handler.registrel(tid1,tid2)
        self.handler.registrelweight(rid)
        if (i == 0) :
          self.handler.registsnext(rid)

    if termsid:
      self.handler.registlterm(termsid[-1])

  def __regist_textlevel(self,text,reply,morph,termsid):
    """
      テキストレベルでの記録を行う。
      文のコンテキストを考慮する上で使うデータとなる

      reply が 2の値の場合はコンテキストの学習には使用されない
    """

    assert ((0 <= reply <= 2))

    if reply == 2 : return None
    return self.handler.registtext(text,morph,0 < reply)


  def regist(self,text,reply=2):
    """
      一文をデータベースに登録するトップレベルのメソッド
      replyはtextが返答に当たるものなのか(システム側の発言)
      ユーザの入力なのか、または語彙を増やすためのものなのかをわける値
      それぞれ1,0,2
    """
    morph = collect(text)
    termsid = map(lambda e: apply(self.handler.registterm , e) , morph)
    self.__regist_termlevel(termsid)
    return self.__regist_textlevel(text,reply,morph,termsid)


  def __fromfile(self,func,dirpath,codec):
    for name in os.listdir(dirpath):
      with open(os.path.join(dirpath,name),"r") as f:
        flag = False
        for line in f.readlines():
          real = map(lambda x: x.strip(),line.decode(codec).split(u"。"))
          for each in real:
            if each:
              self.regist(each,func(flag))
          flag = not flag


  def convcorpus(self,dirpath,codec = "utf-8"):
    self.__fromfile(lambda x: x,dirpath,codec)

  def vocabulary(self,dirpath,codec = "utf-8"):
    self.__fromfile(lambda x:2,dirpath,codec)



class Generate:

  def __init__(self,handler):
    self.handler = handler


  def __iscut(self,length,limit,nid):
    
    #他の終端語と比べて termがどれくらい文尾に来やすいかしらべる
    self.handler.execute("select avg(count) from lastterm")
    (avg,) = self.handler.fetchone()
    self.handler.execute("select count from lastterm where id = ?" , (nid,))
    opt = self.handler.fetchone()
    cnt = opt[0] if opt else 0
    return (length/float(limit)) * (cnt / avg) > 1.0


  def random(self,limitlen,middle=2,default=u"..."):
    """
      コンテキストに寄らないランダムな文章を生成する
      生成のためのデータがどうしても足りない場合は defaultを返す
    """

    self.handler.execute("""
        select count(*) from startnext""")
    head = int(math.ceil(self.handler.fetchone()[0] * 0.8))

    # もっともよく使われる文の始まり方をする
    self.handler.execute("""
           select id from startnext
           order by count desc limit %s""" %(head))
    opt = self.handler.fetchall()

    # 適した文のスタートがない場合はデフォルトで返す
    if not opt : return default

    # 文を作る準備
    text = u""
    rid = random.choice(opt)[0]
    idtuple = self.handler.getrel(rid)
    (sid,nid) = idtuple
    (s,n) = map(self.handler.getterm,idtuple)
    text += s + n

    # 何度も同じフレーズを使ってしまうのを防ぐため
    phrase = set()
    phrase.add((sid,nid))

    while len(text) < limitlen :
      self.handler.execute("""
          select id from relweight as tmp1
          where exists 
            (select id from relmapper where (id1 = ?) and (tmp1.id = id))
          order by count desc limit %s""" %(middle) , (nid,))
      res = self.handler.fetchall()
      if not res: break

      rid = random.choice(res)[0]
      (already , nid) = self.handler.getrel(rid)

      if (not (already,nid) in phrase) : 
        phrase.add((already,nid))

      n = self.handler.getterm(nid)
      text += n
      
      # n がどれくらい 文尾に来やすいか、
      # 現在の文の長さが どれくらい長いかをパラメータとして
      # 適度なところで文を打ち切る
      if self.__iscut(len(text),limitlen,nid): 
        break

    return text


  def __chooseone(self,cand):
    """
      sameメソッドが返す cand = [[textid1,textid2,...] , ...]
      から最も適したtextidを返す
    """

    if random.randint(0,1) == 1:

      rank = {}
      for each in cand:
        if not each: return None
        rank[each[0]] = 0

      for k in rank.keys():
        for c in cand:
          if k in c: rank[k] += 1

      order = sorted(rank.keys(),key = lambda k: -rank[k])
      top = order[0]
      v = rank[top]
      result = random.choice([textid for (textid,val) in rank.items() if val == v])

    else:
      result = random.choice(random.choice(cand))

    return result



  def same(self,textid,distance = 2):
    """
      topicsテーブルより textid と最も似ていると
      いえる文(ただし自分が発言したもの つまり textmapperのreplyフラグは1)のidを求める.
      textidはユーザが入力した文であることを意図していて、
      それと最も似ている自分の過去の発言を探すことで過去に、その次にした
      ユーザの発言を元に文を生成することができる.

      どうしても判断のつかない場合は None を返す
    """
    
    self.handler.execute("""
        select nounlist from textmapper
        where id = ?
        """,(textid,))
    opt = self.handler.fetchone()
    if not opt : 
      return None

    nounlist = pickle.loads(opt[0])
    nounidlist = map(self.handler.gettermid , nounlist)


    top = 3

    # cand の一つづつの要素は nounidlist の中の一つのidで見た時の候補
    # あるnoun で似てる文を考えた時のトップtop件のリスト
    # cand = [[textid1,textid2,...] , ...]
    cand = []
    for tid in nounidlist:
      self.handler.execute("""
          select id2 from topics where 
          ((id1 = ?) and (score > 0) and (abs(id2 - ?) > %s))
          order by score desc limit %s
          """ %(distance,top) , (tid,textid))
      opt = self.handler.fetchall()
      if opt: 
        fmt = [each[0] for each in opt]
        tmp = []
        for candtextid in fmt:
          self.handler.execute("""
              select reply from textmapper 
              where id = ?
              """,(candtextid,))
          (flag,) = self.handler.fetchone() 
          if flag == 1 : 
            tmp.append(candtextid)

        if tmp : cand.append(tmp)
    if not cand : 
      return None

    print cand

    return self.__chooseone(cand)



  def conversation(self,limitlen,context):
    """
      context は 直前のn件でやりたい(textidのリストを取る)んだけど
      今は 1 件で.つまりcontextは直前の文のid
    """

    textid = self.same(context)

    if not textid: 
      return self.random(limitlen)


    target = textid + 1

    self.handler.execute("""
        select text,nounlist from textmapper
        where id = ?
        """,(target,))

    opt = self.handler.fetchone()
    if not opt:
      return self.random(limitlen)

    (text,nounlist) = opt
    nounlist = pickle.loads(nounlist)

    # 今の文脈に最も適切な過去の文が text
    # いまはこれをただかえしてしまっているけど、
    # textを元に文を作って返す

    return text



def initialize(handler):
  handler.initialize()
  register = Regist(handler)
  register.convcorpus("/home/moratori/Github/chain/words/conv-corpus/")
  register.vocabulary("/home/moratori/Github/chain/words/vocabulary/")
  db.Scoring(handler).tfidf()



if __name__ == "__main__":
  with db.chaindb("db/chain.db") as d:
    initialize(d)
    
    #generator = Generate(d)
    #print generator.conversation(60,131).encode("utf-8")


