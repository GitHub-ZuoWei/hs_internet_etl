import os
import json
import redis
import shutil
import pysolr
import platform

import settings as settings
from utils.unzip_file import unzip_file
from utils.mysql_utils import MySqlHelper
from internet.hs_internet_dict_key import InternetJson
from internet.hs_internet_config import hsNewsSql, hsNewsJorSql

"""
      ┏┛ ┻━━━━━┛ ┻┓
      ┃　　　　　　 ┃
      ┃　　　━　　　┃
      ┃　┳┛　  ┗┳　┃
      ┃　　　　　　 ┃
      ┃　　　┻　　　┃
      ┃　　　　　　 ┃
      ┗━┓　　　┏━━━┛
        ┃　　　┃   神兽保佑
        ┃　　　┃   代码无BUG！
        ┃　　　┗━━━━━━━━━┓
        ┃　　　　　　　    ┣┓
        ┃　　　　         ┏┛
        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
          ┃ ┫ ┫   ┃ ┫ ┫
          ┗━┻━┛   ┗━┻━┛
"""

system = platform.system()


class ParseInternet:
    def __init__(self):
        # 美国媒体ID 和 名称
        self.usa_media_section_id_list = []
        self.usa_media_section_name_list = []

        self.hs_news_data_list = []
        self.hs_news_data_list_file = []
        self.hs_news_jour_data_list_file = []
        self.solrLists = []
        self.sql_utils = MySqlHelper()
        self.pysolr_solr = pysolr.Solr(settings.SOLR_HOST)
        self.redis_conn = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB,
                                      decode_responses=True)

    def parser(self):
        internet_json = InternetJson()
        insert_num = 0
        not_insert_num = 0

        # 查询美国的所有媒体
        usa_media_data = self.sql_utils.execute_sql(
            'SELECT mediaSectionId,mediaSectionName FROM hs_media_section WHERE countryNum = "1122"')
        for usa_media_item in usa_media_data:
            self.usa_media_section_id_list.append(usa_media_item[0])
            self.usa_media_section_name_list.append(usa_media_item[1])

        for file_name in os.listdir(settings.ZIP_PATH):
            try:
                parse_response = self.redis_conn.sadd('internet_zip', file_name)
                if parse_response != 1:
                    print(f'Already parse: {file_name}')
                    continue
                unzip_file(settings.ZIP_PATH + ('\\' if system == 'Windows' else '/') + file_name, settings.UNZIP_PATH)
                print(f'成功解压文件:{file_name}')
            except:
                print('unzip error')
                continue
            for root, dirs, files in os.walk(settings.UNZIP_PATH):
                for internetFile in files:
                    fileNamePath = (root + '/' + internetFile).replace('\\', '/')
                    if fileNamePath.split('/')[(-1)] == 'articleData.txt':
                        with open(fileNamePath, encoding='UTF-8', mode='r') as f:
                            try:
                                news_dict = json.loads(f.read(), strict=False)
                            except:
                                print(f'Not JSON :(   Damn!!!  文件路径:{fileNamePath}')
                                # 删除一个已解压的文件目录
                                shutil.rmtree(root, True)
                                continue
                        internet_json.parse_json(fileNamePath, news_dict)
                        # 根据条件过滤新闻
                        validResult = internet_json.internetValid()
                        if validResult is False:
                            continue
                        # 筛选新闻板块
                        if internet_json.siteCofId in self.usa_media_section_id_list or internet_json.siteCofName in self.usa_media_section_name_list:
                            value_tuple = internet_json.get_news_value_tuple(fileNamePath)
                            jour_value_tuple = internet_json.get_jour_value_tuple(news_dict)
                            self.hs_news_data_list_file.append(value_tuple)
                            self.hs_news_jour_data_list_file.append(jour_value_tuple)
                            self.solrLists.append(
                                self.solrInfoDict(jour_value_tuple, value_tuple[4], value_tuple[6]))
                            insert_num += 1
                            # print(f'insert number : {insert_num}')
                        else:
                            not_insert_num += 1
                            # print(not_insert_num)
                        # 删除一个已解压的文件目录
                        shutil.rmtree(root, True)

            # 入一个zip包的解析数据
            self.sql_utils.execute_many_sql(f'Insert Into hs_news_1000{hsNewsSql}', self.hs_news_data_list_file)
            self.sql_utils.execute_many_sql(f'Insert Into hs_data_jor{hsNewsJorSql}', self.hs_news_jour_data_list_file)
            self.sql_utils.conn.commit()
            self.pysolr_solr.add(self.solrLists, commit=True)
            # 清空一个zip包的解析数据
            self.solrLists = []
            self.hs_news_data_list_file = []
            self.hs_news_jour_data_list_file = []

    def solrInfoDict(self, sorlTuple, newsContent, downloadLinks):
        u"""
        solr信息字典化
        """
        solrDict = {}
        solrDict['id'] = sorlTuple[0]
        solrDict['type'] = sorlTuple[1]
        solrDict['tableCode'] = '1000'
        solrDict['title'] = sorlTuple[2]
        solrDict['mediaSectionId'] = sorlTuple[3]
        solrDict['fieldType'] = sorlTuple[4]
        solrDict['libaryFieldType'] = sorlTuple[5]
        solrDict['countryType'] = sorlTuple[6]
        solrDict['languageType'] = sorlTuple[7]
        solrDict['sourceType'] = int(sorlTuple[8])
        solrDict['coverPhoto'] = sorlTuple[9]
        solrDict['newsContent'] = newsContent
        solrDict['downloadLinks'] = downloadLinks
        solrDict['oriTime'] = sorlTuple[11]
        solrDict['sortTime'] = sorlTuple[12]
        solrDict['inTime'] = sorlTuple[13]
        return solrDict


if __name__ == '__main__':
    parser = ParseInternet()
    parser.parser()
    parser.sql_utils.close()
    print('Success ^_^')
