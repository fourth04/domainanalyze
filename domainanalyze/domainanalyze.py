from resolve import *
import signal
import os
from exchange import get_exchange
from actorwrapper import ActorWrapper
from multiprocessing import Queue, Process, Event
import json
from datetime import date, datetime
import conf
from sqlalchemy import create_engine
from sqlalchemy import select, and_, or_, not_, func, text, bindparam
from sqlalchemy.orm import sessionmaker
from models import UrlTask,UrlResult
import logging
from functools import partial
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MYSQL_URI = conf.MYSQL_URI
t_task = UrlTask.__table__
t_result = UrlResult.__table__


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type %s not serializable" % type(obj))

def term(sig_num, addtion):
    """用于父进程异常退出时，将所有子进程都关闭"""
    logger.error('current pid is %s, group id is %s, exception interrupt' % (os.getpid(), os.getpgrp()))
    os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)

def combine_result(q_in, q_out):
    """将各api的查询结果汇总，并保存到数据库中。方法是：
    1. 各api同时向一个队列里放msg，但是每个msg打上类型标签，即往队列中放的msg是dict，key为类型，例如'dns'/'whois'等，value为也是一个dict--key/value分别为url:该url该类型的查询结果
    2. 根据msg的key来区分是什么类型的查询，然后再进行处理，将原来例如{url_like1: {tencent1...}, url_like2: {tencent2...}, ...}的格式转换为{url_like1: {'tencent_info': tencent1...}, url_like2: {'tencent': tencent2...}, ...}
    3. 将相同url的各种查询结果合并在一起，使用了{**{'tencent_info': tencent1}, **{'dns_info': dns1}}的方法

    :q_in: Queue()
    :q_out: Queue()
    :returns: TODO

    """
    while True:
        msg = q_in.get()
        #  msg = q_in.get(timeout=0.1)
        if msg.get('dns', ''):
            dns = {key: {'dns_info': json.dumps(value, default=json_serial)} for key, value in msg['dns'].items()}
        elif msg.get('whois', ''):
            whois = {key: {'whois_info': json.dumps(value, default=json_serial)} for key, value in msg['whois'].items()}
        elif msg.get('icp', ''):
            icp = {key: {'icp_info': json.dumps(value, default=json_serial)} for key, value in msg['icp'].items()}
        elif msg.get('tencent', ''):
            tencent = {key: {'tencent_info': json.dumps(value, default=json_serial), 'category': value['category']} for key, value in msg['tencent'].items()}
        try:
            if all([dns, whois, icp, tencent]):
                result = [
                    {**{'dname': key,
                        'add_time': datetime.now(),
                        'update_time': datetime.now(),
                        'status': 'good'},
                        **dns[key],
                        **whois[key],
                        **icp[key],
                        **tencent[key]} for key in dns]
                del dns, whois, icp, tencent
                q_out.put(result)
        except NameError:
            continue


def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    s_task = select([t_task.c.id, t_task.c.dname]).where(t_task.c.status != 'done').limit(100)
    s_result = select([t_result.c.id, t_result.c.dname])
    u_task = t_task.update().where(t_task.c.id == bindparam('_id')).values({'status': 'done', 'url_result_id': bindparam("url_result_id")})

    engine = create_engine(MYSQL_URI, pool_recycle=1000, encoding='utf-8')

    q_in = Queue()
    q_out = Queue()
    exc = get_exchange('name')
    ps = []
    for func in (dns_resolve_bulk, icp_resolve_bulk, whois_resolve_bulk):
        a = ActorWrapper(partial(func, n=50), q_in)
        a.daemon = True
        a.start()
        ps.append(a)
    b = Process(target=combine_result, args=(q_in, q_out, ))
    b.daemon = True
    b.start()

    signal.signal(signal.SIGTERM, term)

    Session = sessionmaker(bind=engine)
    session = Session()
    with exc.subscribe(*ps):
        while True:
        #  data = [(1, 'www.baidu.com'), (2, 'www.qq.com'), (3, 'www.2134wfewqrwqre.com')]
            try:
                data = session.execute(s_task).fetchall()
                dnames_ids = {x[1]: x[0] for x in data}
                dnames = dnames_ids.keys()
                r_tencent = tencent_resolve_bulk(dnames)['tencent']

                #  过滤出查询结果是安全的结果，将这些dname的记录从t_task表删除
                safe = {key:value for key,value in r_tencent.items() if value['category'] == '安全'}
                if safe:
                    ids_safe = [dnames_ids[k] for k in safe]
                    r_delete = session.execute(t_task.delete().where(t_task.c.id.in_(ids_safe)))
                    logger.info(f"从t_task表删除了{r_delete.rowcount}条记录")

                #  过滤出已在t_result表存在的记录，这些不用再查了，直接将url_result_id关联过来
                not_safe = {key:value for key,value in r_tencent.items() if value['category'] != '安全'}
                not_safe_dnames = not_safe.keys()
                r_select = session.execute(s_result.where(t_result.c.dname.in_(not_safe_dnames)))
                exist_records = {t[1]:t[0] for t in r_select}
                if exist_records:
                    update_data_pre = [{'_id': dnames_ids[k], 'url_result_id': v} for k,v in exist_records.items()]
                    r_update_pre = session.execute(u_task, update_data_pre)
                    logger.info(f"发现已查询过的记录，在t_task表更新了{r_update_pre.rowcount}条记录")

                #  过滤后剩下的需要查询的dname，注意这些记录的tencent_info已经查过了
                filtered_dnames = list(set(dnames) - set(safe) - set(exist_records.keys()))

                if filtered_dnames:
                    #  将腾讯接口查询的结果放到q_in队列中做合并
                    tencent = {'tencent': {key:r_tencent[key] for key in filtered_dnames}}
                    #  icp = icp_resolve_bulk(filtered_dnames)
                    #  whois = whois_resolve_bulk(filtered_dnames)
                    #  dns = dns_resolve_bulk(filtered_dnames)
                    q_in.put(tencent)
                    #  q_in.put(icp)
                    #  q_in.put(whois)
                    #  q_in.put(dns)

                    #  给交换机下发任务
                    exc.send(filtered_dnames)

                    #  获取各接口查询完之后合并的结果
                    resolved_data = q_out.get()
                    bulk_result = [UrlResult(**x) for x in resolved_data]
                    session.bulk_save_objects(bulk_result, return_defaults=True)
                    extracted_result = {result.dname:result.id for result in bulk_result}
                    update_data_suf = [{'_id': dnames_ids[k], 'url_result_id': v} for k,v in extracted_result.items()]
                    r_update_suf = session.execute(u_task, update_data_suf)
                    logger.info(f"在t_task表更新了{r_update_suf.rowcount}条记录")

                session.commit()
            except Exception:
                #  发生异常的话记录异常发生点，并查看是否哪个查询子程序挂了，挂了的话重启
                logging.exception('Catch an exception.')
                for i, p in enumerate(ps):
                    if not p.is_alive():
                        logger.error('{} occured error, trying to reboot it'.format(p.name))
                        ps[i] = ActorWrapper(p.func, q_in)
                        ps[i].start()
                if not b.is_alive():
                    b = Process(target=combine_result, args=(q_in, q_out, ))
                    b.daemon = True
                    b.start()

    for p in ps:
        p.join()
    b.join()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='myapp.log',
                        filemode='w')
    main()
