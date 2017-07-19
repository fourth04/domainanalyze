from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, and_, or_, not_, func, text, bindparam
from datetime import datetime
import json
from models import RegisterTask, RegisterResult
import pandas as pd
import IPy
import tldextract
from conf import MYSQL_URI

t_register_task = RegisterTask.__table__
t_register_result = RegisterResult.__table__

engine = create_engine(MYSQL_URI, encoding='utf-8')

#  dfSource = pd.read_sql_table('register_result_copy', engine)
#  dfSource = dfSource[['domain', 'result']]
#  info = dfSource.result.map(lambda cell: json.loads(cell).get('showapi_res_body', {}).get('obj', {}))
#  df = pd.DataFrame(list(info))
#  df['sld'] = dfSource['domain']

#  df.drop(['user_name', 'domain', 'address'], axis=1, inplace=True)
#  df.drop(df.index[df.num.isnull()], 0, inplace=True)

#  rename_dict = {
    #  'com_name': '主办单位名称',
    #  'num': '网站备案号',
    #  'sys_name': '网站名称',
    #  'type': '主办单位性质',
    #  'update_time': '审核时间',
    #  'sld': '二级域名'
#  }
#  df.rename(columns=rename_dict, inplace=True)
#  cols = ['二级域名', '网站备案号', '主办单位名称', '主办单位性质', '网站名称', '网站首页', '审核时间', '网站标题']
#  df = df.ix[:, cols]
#  df.to_csv('icp.csv', index=False)


rename_dict = {
    'domain': '二级域名',
    'company': '主办单位名称',
    'type': '主办单位性质',
    'icpnum': '网站备案号',
    'updatetime': '审核时间',
}
df1 = pd.read_csv('icp1.csv', encoding='gb18030')
df2 = pd.read_csv('icp2.csv', encoding='gb18030')
df3 = pd.read_csv('icp3.csv', encoding='gb18030')
df4 = pd.read_csv('icp4.csv', encoding='gb18030')
df5 = pd.read_csv('icp5.csv', encoding='gb18030')
df6 = pd.read_csv('icp6.csv', encoding='gb18030')
df6.drop(df6.index[df6['二级域名'].str.contains(u"[\u4e00-\u9fff]+")], inplace=True)
df = pd.concat([df1, df2, df3, df4, df5, df6])
df.drop('网站标题', 1, inplace=True)
rename_dict = {
    '二级域名': 'domain',
    '网站备案号': 'license_number',
    '主办单位名称': 'company_name',
    '主办单位性质': 'company_type',
    '网站名称': 'site_name',
    '网站首页': 'home_page',
    '审核时间': 'audit_time',
}
df.rename(columns=rename_dict, inplace=True)
df['domain'] = df.domain.str.strip()
df['license_number'] = df.license_number.str.strip()
df.drop(df.index[df.domain.isnull()], 0, inplace=True)
df.drop(df.index[df.license_number.isnull()], 0, inplace=True)
df.sort_values(['domain', 'license_number', 'audit_time'], inplace=True)
df.drop_duplicates(['domain'], inplace=True)
df.drop_duplicates(['license_number'], inplace=True)
cols = ['domain', 'license_number', 'company_name', 'company_type', 'site_name', 'home_page', 'audit_time']
df = df.ix[:, cols]
df['add_time'] = datetime.now()
df['update_time'] = datetime.now()
df.reset_index(drop=True, inplace=True)
df.to_csv('icp.csv', index=False)
df.to_sql('icp', engine, if_exists='append',index=True, index_label='id', chunksize=5000)
