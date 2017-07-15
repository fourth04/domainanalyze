from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, and_, or_, not_, func, text, bindparam
from datetime import datetime
import json
import models
import pandas as pd
import IPy
import tldextract
from conf import MYSQL_URI

def dnslog2df(filepath):
    """从一个dns日志加载dataframe，并完成去重

    :file_path: TODO
    :returns: TODO

    """
    df = pd.read_csv(filepath, sep='|', names=['source_ip', 'url', 'timestamp', 'address', 'x', 'y'], na_values=[''], encoding = 'utf-8')
    df.drop_duplicates(['url'], inplace=True)
    df = df.dropna()
    df['dname'] = df['url'].map(lambda x: '.'.join(tldextract.extract(x)[1:]))
    df['address'] = df['address'].str.split(';')
    df['address_int'] = df['address'].map(lambda x: list(map(lambda y: y.int(), map(IPy.IP, x))))
    df['address'] = df['address'].map(json.dumps)
    df['address_int'] = df['address_int'].map(json.dumps)
    df['status'] = 'new'
    df['add_time'] = str(datetime.now())
    df['update_time'] = str(datetime.now())
    df['customer_id'] = 2
    df.drop(['source_ip', 'x', 'y', 'timestamp'], axis=1, inplace=True)
    return df

UrlTask = models.UrlTask
UrlResult = models.UrlResult
task = UrlTask.__table__
result = UrlResult.__table__

engine = create_engine(MYSQL_URI, encoding='utf-8')
Session = sessionmaker(bind=engine)
session = Session()

s_task = select([task.c.id, task.c.url]).where(task.c.status != 'done').limit(50)
i_result = result.insert().prefix_with("IGNORE")

with open('test.json') as f:
    data = json.load(f)

bulk_task = session.query(UrlTask).filter(UrlTask.status!='done').limit(50).all()
bulk_result = [UrlResult(**x) for x in data]
session.bulk_save_objects(bulk_result, return_defaults=True)
#  for task,url_result_id in zip(bulk_task, [x.id for x in bulk_result]):
    #  task.url_result_id = url_result_id
    #  task.status = 'done'
for task,url_result in zip(bulk_task, bulk_result):
    task.url_result = url_result
    task.status = 'done'
session.bulk_save_objects(bulk_task)
session.commit()
