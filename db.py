#!/usr/bin/env python
#coding:utf-8

import sqlite3
import pickle
import math



class db:

  def __init__(self,path):
    self.path = path
    self.con = sqlite3.connect(self.path)
    self.cur = self.con.cursor()

  def execute(self,*arg):
    apply(self.cur.execute,arg)

  def fetchall(self):
    return self.cur.fetchall()
  
  def fetchone(self):
    return self.cur.fetchone()

  def iterate(self,fn,*arg):
    apply(self.execute,arg)
    for row in self.cur:
      fn(row)

  def commit(self):
    self.con.commit()

  def close(self):
    self.cur.close()
    self.con.close()

  def __enter__(self):
    return self

  def __exit__(self,type,value,traceback):
    self.close()



class chaindb(db):

  tables = {"termmapper" :
              """(id integer not null primary key autoincrement, 
                  term text not null, 
                  kind text not null,
                  count integer not null default 1)""",
            "relmapper"  :
              """(id integer not null primary key autoincrement,
                  id1 integer not null, 
                  id2 integer not null, 
                  foreign key (id1) references termmapper(id),
                  foreign key (id2) references termmapper(id))""",
            "relweight" : 
              """(id integer not null primary key,
                  count integer not null default 1, 
                  foreign key (id) references relmapper(id))""",
            "startnext"  :
              """(id integer not null primary key,
                  count integer not null default 1,
                  foreign key (id) references relmapper(id))""",
            "lastterm"   :
              """(id integer not null primary key, 
                  count integer not null default 1,
                  foreign key (id) references termmapper(id))""",
            "textmapper" :
              """(id integer  not null primary key autoincrement, 
                  text text not null,
                  nounlist text not null,
                  reply boolean not null default TRUE)""",
            "topics"     :
              """(id1 integer not null, 
                  id2 integer not null, 
                  score float not null default 0.0,
                  foreign key (id1) references termmapper(id),
                  foreign key (id2) references textmapper(id))"""}

  def __init__(self,path="db/chain.db"):
    db.__init__(self,path)


  def initialize(self):
    """
      現在のデータベースのテーブルを削除して
      全て新しく作りなおす
    """
    for (name,creation) in self.tables.items():
      self.execute("drop table if exists %s" %name)
      self.execute("create table %s %s" %(name,creation))
    self.commit()

  def getterm(self,tid):
    self.execute("select term from termmapper where id = ?",(tid,))
    return self.fetchone()[0]


  def gettermid(self,term):
    """
      termmapperの語からIDを求める
      語が存在しない場合はNoneが返る

      term は Unicode型
    """
    self.execute('select id from termmapper where term = ?' , (term,))
    res = self.fetchone()
    if res:
      return res[0]
    else: return None


  def registterm(self,term,kind):
    """
      termmapperテーブルに新しく語を追加する
      すでに存在する場合は何もしない。
      いづれの場合も語のIDを返す

      term,kind は Unicode型
    """
    already = self.gettermid(term)
    if already: 
      self.execute("""
           update termmapper set count = count + 1 
           where id = ?""" , (already,))
      self.commit()
      return already

    self.execute("""
        insert into termmapper(term,kind)
        values(?,?)""",
        (term,kind))
    self.commit()
    return self.gettermid(term)


  def getrel(self,rid):
    self.execute("select id1,id2 from relmapper where id = ?" , (rid,))
    return self.fetchone()


  def getrelid(self,tid1,tid2):
    """
      relmapperテーブルから２つの語の関係を表す
      idを求める
    """
    self.execute("""
        select id from relmapper
        where (id1 = ?) and (id2 = ?)""",
        (tid1,tid2))
    res = self.fetchone()
    if res:
      return res[0]
    else: return None


  def getsamekind(self,spec,limit = 10):
    """
      品詞の同じ語を取得する
    """
    self.execute("""select id,term from termmapper
                  where kind = ? limit %s""" %(limit) , (spec,))
    return self.fetchall()


  def registrel(self,tid1,tid2):
    """
      relmapperにtid1とtid2の関係を登録し
      そのIDを返す。
    """
    already = self.getrelid(tid1,tid2)
    if already : return already

    self.execute("""
        insert into relmapper(id1,id2)
        values(?,?)
        """,
        (tid1,tid2))
    self.commit()
    return self.getrelid(tid1,tid2)

  def __getcount(self,tablename,ids):
    """
      relweight,startnext,lasttermのテーブルのいづれもcountをもっており
      同様に取得できるので
    """
    self.execute("select count from %s where id = ?" %(tablename) , (ids,))
    res = self.fetchone()
    if res:
      return res[0]
    else: return None


  def __increlcount(self,rid,tablename):
    """
      relweightテーブルとstartnextテーブルのカウントアップ処理
      は殆ど同じなので、 registrelweight , registsnext , registltermメソッドは
      このメソッドを呼び出す
    """
    res = self.__getcount(tablename,rid)
    if res:
      self.execute("update %s set count = count + 1 where id = ?" %(tablename) , (rid,))
    else:
      self.execute("insert into %s(id) values(?)" %(tablename), (rid,))
    self.commit()


  def registrelweight(self,rid):
    """
      語の関係の重さを記録する
    """
    self.__increlcount(rid,"relweight")

  def registsnext(self,rid):
    """
      文のはじめの語の関係を記録する
    """
    self.__increlcount(rid,"startnext")

  def registlterm(self,tid):
    """
      文の終端となる語を記録する
    """
    self.__increlcount(tid,"lastterm")

  def registtext(self,text,morph,reply):
    # nounlist は text 中の名詞 
    nounlist = [each[0] for each in morph if 
        ((each[1] == u"名詞") or (each[1] == u"動詞") or (each[1] == u"感動詞"))]
        # ここは tfidfメソッドに依存しまくっている


    self.execute("""
        insert into textmapper(text,nounlist,reply) values(?,?,?)
        """ , (text,pickle.dumps(nounlist),reply))
    self.commit()
    self.execute("""
        select id from textmapper where text = ?
        order by id desc limit 1
        """,(text,))
    return self.fetchone()[0]



class Scoring:
  
  """
    term について最もよく言及している文章はどれかを
    判断する指標として tf・idf値等を計算するクラス
  """

  def __init__(self,handler):
    self.handler = handler

  def __erase(self):
    definition = chaindb.tables["topics"]
    self.handler.execute("""
      drop table if exists topics""")
    self.handler.execute("""
      create table topics %s
        """ %(definition))
    self.handler.commit()


  def tfidf(self):
    """
      過去の tfidf値が入ってる topics テーブルは削除し
      新しく計算しなおす
    """

    self.__erase()


    self.handler.execute("""
        select id,term from termmapper where (kind = ?) or (kind = ?) or (kind = ?)
        """,(u"名詞",u"動詞",u"感動詞"))


    allnounlist = self.handler.fetchall()
    self.handler.execute("""
        select id,nounlist from textmapper
        """)
    alltextlist = map(lambda e: (e[0],pickle.loads(e[1])),self.handler.fetchall())
    if not allnounlist : return 

    self.handler.execute("""
        select count(*) from textmapper
        """)
    (n,) = self.handler.fetchone()

    tftable = {}
    idftable = {}
    for (termid,term) in allnounlist:
      for (textid,nounlist) in alltextlist:
        # nounlist の長さに応じて　適当に正規化してやる必要あり
        tftable[(termid,textid)] = nounlist.count(term)
        if term in nounlist:
          if idftable.get(termid,None):
            idftable[termid] += 1
          else:
            idftable[termid] = 1
      # term が　いずれの nounlistにも出現せず、
      # idftable[termid] が keyエラーになることは普通にある
      # なぜならばvocabulary習得のためにDBに突っ込んだものは
      # termmapper には存在するが、 textには存在しないことになる
      idftable[termid] = 0 if not idftable.get(termid,None) else math.log((n/idftable[termid])+1)

    for (termid,term) in allnounlist:
      for (textid,nounlist) in alltextlist:
        val = tftable[(termid,textid)] * idftable[termid]
        self.handler.execute("""
            insert into topics values(?,?,?)
            """,(termid,textid,val))
    self.handler.commit()










