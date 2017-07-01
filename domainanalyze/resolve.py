import re
from urllib.parse import urlparse
import dns.resolver
import dns.rdtypes.IN.A
import whois
import tldextract
import requests
import itertools
#  from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from collections import ChainMap
import conf
import IP

IPADR_PATTERN = re.compile(r'^(?:\d+\.){3}\d+')
TENCENT_JWT = conf.TENCENT_JWT
TENCENT_URL = conf.TENCENT_URL
URLTYPE_DICT = conf.URLTYPE_DICT
EVILCLASS_DICT = conf.EVILCLASS_DICT
SHOWAPI_URL = conf.SHOWAPI_URL
SHOWAPI_APPID = conf.SHOWAPI_APPID
SHOWAPI_SIGN = conf.SHOWAPI_SIGN

def multithreading(n):
    def wrapper(func):
        def inner(urls):
            with ThreadPoolExecutor(n) as pool:
                return pool.map(func, urls)
        return inner
    return wrapper

def location_resolve(url_like):
    """根据输入的url_like，查询其对应的归属地

    :url_like: 可能有几种形式：url: http://www.baidu.com，domain: www.baidu.com，ip: 14.15.15.17，甚至可能是：http://14.15.16.17:80
    :returns: {url_like: [location]}

    """
    location_s = IP.find(url_like)
    location_l = location_s.split('\t') if location_s else []
    n = len(location_l)
    headers = ['country', 'province', 'city', 'carrier']
    if n < 4:
        location = dict(zip(headers, location_l + list(itertools.repeat('', 4-n))))
    elif n == 4:
        location = dict(zip(headers, location_l))
    else:
        location = dict(zip(headers, location_l[:3] + location_l[-1:]))
    return {url_like: location}

def location_resolve_bulk(urls, n=30):
    """批量解析url_like的whois

    :urls: [url_like...]
    :returns: {url_like1: {location1...}, url_like2: {location2...}, ...}

    """
    with ThreadPoolExecutor(n) as pool:
        result = pool.map(location_resolve, urls)
        return {'location': ChainMap(*result)}

#  @multithreading(30)
def dns_resolve(url_like):
    """根据输入的url_like，查询其对应的ip，因为dns模块只能查domain格式的数据，所以需要将url_like转换成domain。另外，如果是domain是ip，则直接截取ip即可

    :url_like: 可能有几种形式：url: http://www.baidu.com，domain: www.baidu.com，ip: 14.15.15.17，甚至可能是：http://14.15.16.17:80
    :returns: {url_like: [ip]}

    """
    url_parse_result = urlparse(url_like)
    domain = url_parse_result.hostname if url_parse_result.scheme else url_like
    if IPADR_PATTERN.search(domain):
        return {url_like: [domain]}
    else:
        try:
            a = dns.resolver.query(domain, 'A').response.answer
            #  即便是只查A记录，最后还是可能会出dns.rdtypes.ANY.CNAME.CNAME类型的记录，所以需要判断是否是dns.rdtypes.IN.A.A
            ips = [j.address for i in a for j in i.items
                   if isinstance(j, dns.rdtypes.IN.A.A)]
        except Exception:
            return {url_like: []}
        else:
            return {url_like: ips}


def dns_resolve_bulk(urls, n=30):
    """批量解析url_like的ip地址

    :urls: [url_like...]
    :returns: {url_like1: [ip1...], url_like2: [ip2...], ...}

    """
    with ThreadPoolExecutor(n) as pool:
        result = pool.map(dns_resolve, urls)
        return {'dns': ChainMap(*result)}


#  @multithreading(30)
def whois_resolve(url_like):
    """输入url_like，查询whois信息。
    使用whois模块过程中发现，whois模块并不会抛出异常，只会打印出异常，出现异常的时候，查询到的所有whois结果为None。
    此外，whois会返回两种格式的结果，一般是18个字段的结果，但是有些查询结果会出现更为详尽的61个字段(详尽记录了admin/registrant/tech/trademark，即管理员、注册者等的各种记录，怀疑是老版本的记录格式)，所以需要统一字段，才方便存入数据库，我们将这种61个字段的主要取其注册者信息，从而精简为18个字段。
    后来又发现居然有64个字段的返回结果，也是醉了！发现多了registrant_address1，少了registrant_address和registrant_org

    :url_like: 可能有几种形式：url: http://www.baidu.com，domain: www.baidu.com，ip: 14.15.15.17，甚至可能是：http://14.15.16.17:80
    :returns: {url_like: whois}

    """
    sld = '.'.join(tldextract.extract(url_like)[1:])
    w = whois.whois(sld)
    if len(w) > 25:
        if 'registrant_address1' in w:
            w['registrant_address'] = w['registrant_address1' ]
        if 'registrant_org' not in w:
            w['registrant_org'] = None
        w = {
            'address': w['registrant_address'],
            'city': w['registrant_city'],
            'country': w['registrant_country'],
            'creation_date': w['creation_date'],
            'dnssec': None,
            'domain_name': w['domain_name'],
            'emails': w['registrant_email'],
            'expiration_date': w['expiration_date'],
            'name': w['registrant_name'],
            'name_servers': w['name_servers'],
            'org': w['registrant_org'],
            'referral_url': None,
            'registrar': w['registrar'],
            'state': w['registrant_state_province'],
            'status': w['status'],
            'updated_date': w['updated_date'],
            'whois_server': None,
            'zipcode': w['registrant_postal_code'],
        }
    return {url_like: w}


def whois_resolve_bulk(urls, n=30):
    """批量解析url_like的whois

    :urls: [url_like...]
    :returns: {url_like1: {whois1...}, url_like2: {whois2...}, ...}

    """
    with ThreadPoolExecutor(n) as pool:
        result = pool.map(whois_resolve, urls)
        return {'whois': ChainMap(*result)}

#  @multithreading(30)
def tencent_resolve(url_like):
    """根据输入的url_like，查询其腾讯安全接口查询结果

    :url_like: 可能有几种形式：url: http://www.baidu.com，domain: www.baidu.com，ip: 14.15.15.17，甚至可能是：http://14.15.16.17:80
    :returns: {url_like: tencent}

    """
    url = TENCENT_URL
    payload = {
        'dname': url_like
    }
    headers = {
        'Authorization': 'Bearer {}'.format(TENCENT_JWT)
    }
    r = requests.get(url, params=payload, headers=headers)
    if r.ok:
        data = r.json()
        evilclass_code = data['data']['evilclass']
        urltype_code = 0 if data['data']['urltype'] == 9 else data['data']['urltype']
        evilclass = EVILCLASS_DICT.get(evilclass_code, '未知')
        urltype = URLTYPE_DICT.get(urltype_code, '未知')
        data['data']['evilclass'] = evilclass
        data['data']['urltype'] = urltype
        data['data']['category'] =  evilclass if urltype != '安全' else urltype
        d = data['data']
        return {url_like: d}
    else:
        return {url_like: {'dname': url_like,'category': '未知', 'urltype': '未知', 'evilclass': '未知'}}


def tencent_resolve_bulk(urls, n=30):
    """批量解析url_like的安全信息

    :urls: [url_like...]
    :returns: {'tencent': {url_like1: {tencent1...}, url_like2: {tencent2...}, ...}}

    """
    with ThreadPoolExecutor(n) as pool:
        result = pool.map(tencent_resolve, urls)
        return {'tencent': ChainMap(*result)}


def icp_resolve(url_like):
    """根据输入的url_like，查询其icp备案信息

    :url_like: 可能有几种形式：url: http://www.baidu.com，domain: www.baidu.com，ip: 14.15.15.17，甚至可能是：http://14.15.16.17:80
    :returns: {url_like: icp}

    """
    url = SHOWAPI_URL
    payload = dict(showapi_appid=SHOWAPI_APPID, showapi_sign=SHOWAPI_SIGN, domain=url_like)
    try:
        r = requests.post(url, payload)
        icp = r.json()
    except Exception:
        icp = {url_like: {'showapi_res_body': {}, 'showapi_res_code': -1, 'showapi_res_error': ''}}
    finally:
        return {url_like: icp}

def icp_resolve_bulk(urls, n=30):
    """批量解析url_like的icp

    :urls: [url_like...]
    :returns: {url_like1: {icp1...}, url_like2: {icp2...}, ...}

    """
    with ThreadPoolExecutor(n) as pool:
        result = pool.map(icp_resolve, urls)
        return {'icp': ChainMap(*result)}


#  def icp_resolve_bulk(urls):
    #  """icp备案情况查询。利用tldextract过滤掉不规范的urls，例如123456.123456，这类域名，顶级域名通过tldextract.extract('http://forums.bbc.co.uk').suffix获取，当它为空时则代表不规范域名

    #  @param urls: 多个域名
    #  @type  urls: iterable

    #  @return: {url_like1: {icp1...}, url_like2: {icp2...}, ...}
    #  @rtype :  dict
    #  """
    #  urls_irregular = [url for url in urls if not tldextract.extract(url).suffix]
    #  result_irregular = {url: {} for url in urls_irregular}
    #  urls_regular = set(urls) - set(urls_irregular)

    #  url = 'http://icp.chinaz.com/searchs'
    #  headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    #  payload = {
        #  'btn_search': '查询',
        #  'urls': '\r\n'.join(urls_regular)
    #  }
    #  r = requests.post(url, headers=headers, data=payload)
    #  html = etree.HTML(r.text)
    #  trs = html.xpath('//*[@id="result_table"]/tr')
    #  l_text = (tr.xpath('./td//text()') for tr in trs)
    #  named = (dict(zip(('organizer_name',
                       #  'property',
                       #  'license',
                       #  'site_name',
                       #  'home_page',
                       #  'review_time'),
                      #  text)) if '--' not in text else {} for text in l_text)
    #  result_regular = dict(zip(urls_regular, named))
    #  result_regular.update(result_irregular)
    #  return {'icp': ChainMap(result_regular)}


if __name__ == "__main__":
    urls = [line.strip() for line in open('./urls/raw_urls.txt')]
    dnames = [urlparse(url).hostname for url in urls]
