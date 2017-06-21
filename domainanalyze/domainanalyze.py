from resolve import *
from exchange import get_exchange
from actorwrapper import ActorWrapper
from multiprocessing import Queue, Process, Event
import json
from datetime import date, datetime
import conf
from sqlalchemy import create_engine
from sqlalchemy import select, and_, or_, not_, func, text, bindparam
import models

MYSQL_URI = conf.MYSQL_URI
task = models.UrlTask.__table__
result = models.UrlResult.__table__


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type %s not serializable" % type(obj))


def saveResult(q, e):
    """将各api的查询结果汇总，并保存到数据库中。方法是：
    1. 各api同时向一个队列里放msg，但是每个msg打上类型标签，即往队列中放的msg是dict，key为类型，例如'dns'/'whois'等，value为也是一个dict--key/value分别为url:该url该类型的查询结果
    2. 根据msg的key来区分是什么类型的查询，然后再进行处理，将原来例如{url_like1: {tencent1...}, url_like2: {tencent2...}, ...}的格式转换为{url_like1: {'tencent_info': tencent1...}, url_like2: {'tencent': tencent2...}, ...}
    3. 将相同url的各种查询结果合并在一起，使用了{**{'tencent_info': tencent1}, **{'dns_info': dns1}}的方法

    :q: Queue()
    :e: Event()
    :returns: TODO

    """
    engine = create_engine(MYSQL_URI, pool_recycle=1000, encoding='utf-8')
    with engine.connect() as conn:
        while True:
            msg = q.get()
            if msg.get('dnames', ''):
                dnames = {
                    key: {
                        'dname': value} for key,
                    value in msg['dnames'].items()}
            elif msg.get('dns', ''):
                dns = {
                    key: {
                        'dns_info': json.dumps(
                            value,
                            default=json_serial)} for key,
                    value in msg['dns'].items()}
            elif msg.get('whois', ''):
                whois = {
                    key: {
                        'whois_info': json.dumps(
                            value,
                            default=json_serial)} for key,
                    value in msg['whois'].items()}
            elif msg.get('tencent', ''):
                tencent = {
                    key: {
                        'tencent_info': json.dumps(
                            value,
                            default=json_serial),
                        'category': value['data']['category']} for key,
                    value in msg['tencent'].items()}
            elif msg.get('icp', ''):
                icp = {
                    key: {'icp': json.dumps(value, default=json_serial)}
                    for key, value in msg['icp'].items()}
            try:
                if all([dnames, dns, whois, tencent, icp]):
                #  if all([dnames, dns, whois, tencent]):
                    result = [{**{'url': key, 'add_time': datetime.now(), 'update_time': datetime.now(), 'status': 'good'},
                               **dnames[key],
                               **dns[key],
                               **whois[key],
                               #  **icp[key],
                               **tencent[key]} for key in dns]
                    with open('test.json', 'w') as f:
                        json.dump(result, f, default=json_serial)
                    del dnames, dns, whois, tencent, icp
                    #  del dnames, dns, whois, tencent
                    e.set()
            except NameError:
                continue


def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    s_task = select(
        [task.c.id, task.c.url, task.c.dname]).where(
        task.c.status != 'done').limit(50)
    engine = create_engine(MYSQL_URI, pool_recycle=1000, encoding='utf-8')

    q = Queue()
    e = Event()
    exc = get_exchange('name')
    a1 = ActorWrapper(dns_resolve_bulk, q)
    a2 = ActorWrapper(tencent_resolve_bulk, q)
    a3 = ActorWrapper(whois_resolve_bulk, q)
    a4 = ActorWrapper(icp_resolve_bulk, q)
    a1.start()
    a2.start()
    a3.start()
    a4.start()
    b = Process(target=saveResult, args=(q, e, ))
    b.start()

    with engine.connect() as conn:
        with exc.subscribe(a1, a2, a3, a4):
        #  with exc.subscribe(a1, a2, a3):
            #  while True:
            data = conn.execute(s_task).fetchall()
            dnames = {'dnames': {x[1]: x[2] for x in data}}
            q.put(dnames)
            urls = [x[1] for x in data]
            e.clear()
            exc.send(urls)
            e.wait()
    a1.join()
    a2.join()
    a3.join()
    a4.join()
    b.join()

if __name__ == "__main__":
    main()
