import datetime
import pandas as pd
import psycopg2

#データベース接続とファイル読み込み
conn= psycopg2.connect("host=127.0.0.1 port=5432 dbname=video_research user=yamashita_lab password=irohasu")
print("データベース接続完了")
cur = conn.cursor()
#ファイル保管場所のリスト
filelist=["t_weblog_0000_part_00.tsv","t_weblog_0001_part_00.tsv","t_weblog_0002_part_00.tsv","t_weblog_0003_part_00.tsv"]
#table情報
table_name="WEBサイト閲覧ログ"
field=[
    ["世帯No","varchar(10)"],
    ["個人No","varchar(10)"],
    ["日付" ,"date"],
    ["時刻", "varchar(255)"],
    ["PCフラグ","char(1)"],
    ["SPフラグ","char(1)"],
    ["TBフラグ","char(1)"],
    ["URL","text"],
    ["ドメイン","text"],
    ["サブドメイン","text"],
    ["レファラー","text"],
    ["リファラーのドメイン","text"],
    ["ページのタイトル","text"],
    ["滞在時間","int"]
]

#create文を作成
sql_createtable="create table "+table_name+"("
for x in field:
    sql_createtable=sql_createtable+x[0]+" "+x[1]+","
sql_createtable=sql_createtable[0:-1]+");"


#insert文を作成
sql_insert="insert into "+table_name+"("
for x in field:
    sql_insert=sql_insert+x[0]+","
sql_insert=sql_insert[0:-1]+") values("
for x in field:
    sql_insert=sql_insert+"%s,"
sql_insert=sql_insert[0:-1]+");"
print(sql_createtable)
print(sql_insert)


#実行文
cur.execute("rollback")
cur.execute("drop table if exists "+table_name+";")
cur.execute(sql_createtable)
for filename in filelist:
    df=pd.read_csv(filename,header=None,dtype=str,delimiter='\t' )
    df=df.fillna("NULL")
    dl=df.values.tolist()
    print("ファイル:"+filename+"読込完了")
    print(df.shape)
    del df
    for x in dl:
        cur.execute(sql_insert,x)
    del dl
conn.commit()
cur.close()
conn.close()
