import time
import json
from resolve import icp_resolve_bulk, socket_resolve_bulk
from sqlalchemy import create_engine
from sqlalchemy import select, and_, or_, not_, func, text, bindparam
from sqlalchemy.orm import sessionmaker
import conf
from datetime import datetime
from models import RegisterTask, RegisterResult
from utils import json_serial

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MYSQL_URI = conf.MYSQL_URI
t_task = RegisterTask.__table__
t_result = RegisterResult.__table__


def main():
    """TODO: Docstring for main.
    :returns: TODO

    """
    s_task = select([t_task.c.id, t_task.c.domain, t_task.c.names]).where(t_task.c.status == 'new').limit(4)
    s_result = select([t_result.c.id, t_result.c.domain])
    u_task = t_task.update().where(t_task.c.id == bindparam('_id')).values({'status': 'done', 'register_result_id': bindparam("register_result_id")})

    engine = create_engine(MYSQL_URI, pool_recycle=1000, encoding='utf-8')

    Session = sessionmaker(bind=engine)
    session = Session()

    while True:
        try:
            data = session.execute(s_task).fetchall()
            n_data = len(data)
            logger.info(f"从register_task表获取了{n_data}条记录")
            if not n_data:
                logger.info(f"休眠4分钟")
                time.sleep(4 * 60)
                continue
            dnames_ids = {x[1]: x[0] for x in data}
            dnames_names = {x[1]: x[2] for x in data}
            dnames = dnames_ids.keys()

            #  r_select = session.execute(s_result.where(t_result.c.domain.in_(dnames)))
            #  exist_records = {t[1]:t[0] for t in r_select}
            #  if exist_records:
                #  update_data_pre = [{'_id': dnames_ids[k], 'register_result_id': v} for k,v in exist_records.items()]
                #  r_update_pre = session.execute(u_task, update_data_pre)
                #  logger.info(f"发现已查询过的记录，在register_task表更新了{r_update_pre.rowcount}条记录")

            #  filtered_dnames = list(set(dnames) - set(exist_records.keys()))
            #  用于测试
            filtered_dnames = dnames

            if filtered_dnames:
                logger.info(f"开始进行icp接口查询")
                r_icp = icp_resolve_bulk(filtered_dnames, 50)['icp']
                logger.info(f"icp接口查询完毕")
                #  icp_resolved_data = ({'status': 'good', 'domain':key, 'register_status': 'yes' if value.get('showapi_res_body', {}).get('ret_code', -1) != -1 else 'no', 'result': json.dumps(value, default=json_serial), 'add_time': datetime.now(), 'update_time': datetime.now()} for key, value in r_icp.items())

                filtered_dnames_names = [{k:v} for k,v in dnames_names.items() if k in filtered_dnames]
                logger.info(f"开始进行socket接口查询")
                r_socket = socket_resolve_bulk(filtered_dnames_names, 50)['socket']
                logger.info(f"socket接口查询完毕")

                resolved_data = ({'status': 'good', 'domain':key, 'register_status': 'yes' if value.get('showapi_res_body', {}).get('ret_code', -1) != -1 else 'no', 'result': json.dumps(value, default=json_serial), 'add_time': datetime.now(), 'update_time': datetime.now(), **r_socket[key]} for key, value in r_icp.items())

                bulk_result = [RegisterResult(**x) for x in resolved_data]
                session.bulk_save_objects(bulk_result, return_defaults=True)
                extracted_result = {result.domain:result.id for result in bulk_result}
                update_data_suf = [{'_id': dnames_ids[k], 'register_result_id': v} for k,v in extracted_result.items()]
                r_update_suf = session.execute(u_task, update_data_suf)
                logger.info(f"在register_result表插入了{r_update_suf.rowcount}条记录，在register_task表更新了{r_update_suf.rowcount}条记录")

            session.commit()
        except Exception:
            session.rollback()
            logger.error("数据库操作过程中遇错，退出", exc_info=True)
            break


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        #  filename='register.log',
                        #  filemode='w'
                        )
    main()
