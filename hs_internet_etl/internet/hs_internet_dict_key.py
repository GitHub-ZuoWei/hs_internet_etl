import json
import os
import shutil
import time
import uuid
from datetime import datetime
import langid

from hs_internet_etl.utils.mysql_utils import MySqlHelper


class InternetJson(object):
    def __init__(self):
        self.sql_help = MySqlHelper()

        # 初始化Mysql数据字典
        self.fieldRuleLists = []
        self.ruleItemDict = {}
        self.sourceDict = {}
        self.numByLanguageCodeDict = {}
        self.numByLanguageDict = {}
        self.mediaByMediaSectionDict = {}
        self.languageNumByMediaSectionDict = {}
        self.tagsByMediaSectionDict = {}
        self.countryNumByMediaSectionDict = {}
        self.regionNumByMediaSectionDict = {}
        self.mediaSectionDict = {}
        self.mediaSectionNameDict = {}
        self.mediaSectionIdByNameDict = {}
        self.mediaSectionNameBySectionId = {}

        # 查Mysql 获取字典信息
        self.getRuleItemDict()
        self.getFieldRule()
        self.getDictOfMediaSection()
        self.getSourceTypeDict()
        self.getNumByLanguageCodeDict()

    def parse_json(self, file_name, news_dict):
        self.__img = None
        self.__localImgPath = None
        self.__attributeJson = None
        self.__internetFieldType = None
        self.__fileLists = []

        self.uuid = str(uuid.uuid1()).replace('-', '')
        self.author = news_dict.get('author')
        self.clickNumber = news_dict.get('clickNumber')
        self.comment = news_dict.get('comment')
        self.content = news_dict.get('content')
        self.contentForExport = news_dict.get('contentForExport')
        self.contentShow = news_dict.get('contentShow')
        self.contentTranslation = news_dict.get('contentTranslation')
        self.creationTime = news_dict.get('creationTime')
        self.dataType = news_dict.get('dataType')
        self.encoding = news_dict.get('encoding')
        self.excavateDataFileList = news_dict.get('excavateDataFileList')
        self.guid = news_dict.get('guid')
        self.imgList = news_dict.get('imgList')
        self.keywords = news_dict.get('keywords')
        self.metaData = news_dict.get('metaData')
        self.publicDate = news_dict.get('publicDate')
        dataTime = news_dict.get('publicDateTime')
        self.publicTime = news_dict.get('publicTime')
        self.refernceUrl = news_dict.get('refernceUrl')
        self.region = news_dict.get('region')
        self.replies = news_dict.get('replies')
        self.reproduced = news_dict.get('reproduced')
        self.siteCofId = news_dict.get('siteCofId')
        self.siteCofName = news_dict.get('siteCofName')
        self.taskDataId = news_dict.get('taskDataId')
        self.title = news_dict.get('title')
        self.titleTranslation = news_dict.get('titleTranslation')
        self.url = news_dict.get('url')
        self.videoList = news_dict.get('videoList')
        self.inTime = time.strftime('%Y-%m-%d %H:%M:%S')

        if dataTime != None and dataTime != '':
            if len(dataTime.strip()) == 19:
                self.publicDateTime = dataTime
            elif len(dataTime.strip()) == 10:
                self.publicDateTime = dataTime.strip() + ' 00:00:00'
        else:
            self.publicDateTime = news_dict.get('creationTime')

        excavateDataFileList = news_dict.get('excavateDataFileList')
        if excavateDataFileList:
            for fileDict in excavateDataFileList:
                if fileDict['fileName'].split('.')[-1] == 'pdf':
                    self.__fileLists.append(fileDict['fileName'])
                elif fileDict['fileName'].split('.')[-1] == 'jpg':
                    self.__img = fileDict.get('fileName')
        if self.__img:
            self.savePicOrFile(file_name, '/data/images', 'lib_pic_jor')
            # self.savePicOrFile(file_name, 'D:/tmp', 'lib_pic_jor')
        else:
            if self.__fileLists:
                self.savePicOrFile(file_name, '/data/images', 'lib_file_jor')
                # self.savePicOrFile(file_name, 'D:/tmp', 'lib_file_jor')
        return

    def get_news_value_tuple(self, file_name_path):
        u"""
                新闻详情表hs_news
        """

        value_tuple = (
            self.uuid, '1', self.title, self.publicDateTime,
            self.contentShow.replace('\n', '').strip(), self.__localImgPath, self.__attributeJson, self.url,
            '0', self.creationTime, self.titleTranslation, self.contentTranslation,
            self.inTime, None, file_name_path)

        return value_tuple

    def get_jour_value_tuple(self, news_dict):
        u"""
                新闻详情表hs_data_jour
        """
        mediaSectionId = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.siteCofId)).replace('-', '')
        if mediaSectionId not in self.mediaSectionDict:
            mediaSectionId = self.mediaSectionIdByNameDict.get(self.siteCofName)

        mediaId = self.mediaByMediaSectionDict.get(mediaSectionId)
        countryNum = self.countryNumByMediaSectionDict.get(mediaSectionId)
        if mediaId != 'c539e686edf25845b893f83af27eebc5':
            sourceType = self.sourceDict.get('互联网')
        else:
            sourceType = self.sourceDict.get('微信公众号')

        str(uuid.uuid5(uuid.NAMESPACE_DNS, 'b0131843bdbf4ffda4ec7a5c9c0af58f')).replace('-', '')

        tags = self.tagsByMediaSectionDict.get(mediaSectionId)

        self.languageType = self.languageNumByMediaSectionDict.get(mediaSectionId)
        if self.languageType == None:
            languageCode = langid.classify(self.contentShow)[0]
            self.languageType = self.numByLanguageCodeDict.get(languageCode)

        internetFieldType = self.fieldTypeByRule(countryNum, sourceType, mediaSectionId, tags, mediaId,
                                                 self.languageType, None, None, None,
                                                 '%s %s' % (self.title, self.contentForExport))
        value_tuple = (
            self.uuid, '1', self.title, mediaSectionId, internetFieldType, None,
            countryNum, self.languageType, sourceType,
            self.__localImgPath, 1, self.publicDateTime, self.creationTime, self.creationTime)

        return value_tuple

    def internetValid(self):
        u"""
                数据验证
        """
        newsTimeOfDateTime = datetime.strptime(self.publicDateTime, '%Y-%m-%d %H:%M:%S')
        nowTimeOfDataTime = datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
        if self.isVaildDate(self.publicDateTime) == False:
            return False
        else:
            if self.isVaildDate(self.creationTime) == False:
                return False
            if not self.title or self.title[-4:].lower() == '.jpg' \
                    or self.title[-5:].lower() == '.jpeg' or self.title[-4:].lower() == '.pdf':
                return False
            if newsTimeOfDateTime > nowTimeOfDataTime:
                return False
            # titleId = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.__title.lower())).replace('-', '')
            # element = self.redis.insert(name=constConfig.redis_internet_titles, value=titleId, kind='set')
            # if element == 0:
            #     return False
            return True

    def isVaildDate(self, date):
        u"""
                校验时间规则，多加一个长度判断，因为solr中对于date日期严格。
                例如："2019-08-4 16:58:00" ，solr识别出不是date格式
        """
        try:
            if ':' in date:
                time.strptime(date, '%Y-%m-%d %H:%M:%S')
                if len(date) != 19:
                    return False
            else:
                time.strptime(date, '%Y-%m-%d')
                if len(date) != 10:
                    return False
            return True
        except:
            return False

    def getSourceTypeDict(self):
        u"""
                获取来源类型字典
        """
        sourceTypeSql = 'select sourceName,sourceCode from hs_source '
        results = self.sql_help.execute_sql(sourceTypeSql)
        for row in results:
            self.sourceDict[row[0]] = str(row[1])

    def getNumByLanguageCodeDict(self):
        u"""
                获取语种编号字典code：number
        """
        languageSql = 'select number,code,language from hs_languages '
        results = self.sql_help.execute_sql(languageSql)
        for row in results:
            self.numByLanguageCodeDict[row[1]] = str(row[0])
            for lan in row[2].split('、'):
                self.numByLanguageDict[lan] = str(row[0])

    def getFieldRule(self):
        fieldRuleSql = "SELECT a.fieldId,GROUP_CONCAT(a.itemId,':',a.contentRelation,':',a.content separator '&')             from hs_field_rule a             LEFT JOIN hs_field b             on a.fieldId = b.fieldId             where a.isDelete = '0' and b.isDelete = '0'             group by a.fieldRelId             ORDER BY b.bk1"
        results = self.sql_help.execute_sql(fieldRuleSql)
        if len(results) != 0:
            for fieldRule in list(results):
                fieldRuleDict = {}
                fieldRuleItemLists = []
                fieldRuleDict['fieldId'] = fieldRule[0]
                ruleItemLists = fieldRule[1].split('&')
                for ruleItemList in ruleItemLists:
                    fieldRuleItemDict = {}
                    itemId = ruleItemList.split(':')[0]
                    contentRelation = ruleItemList.split(':')[1]
                    content = ruleItemList.split(':')[2]
                    fieldRuleItemDict['contentRelation'] = contentRelation
                    fieldRuleItemDict['itemId'] = self.ruleItemDict[itemId]
                    fieldRuleItemDict[self.ruleItemDict[itemId]] = content
                    fieldRuleItemLists.append(fieldRuleItemDict)

                fieldRuleDict['fieldRuleItem'] = fieldRuleItemLists
                self.fieldRuleLists.append(fieldRuleDict)

    def getRuleItemDict(self):

        ruleItemSql = 'SELECT id,spiderName from hs_field_rule_item '
        results = self.sql_help.execute_sql(ruleItemSql)
        for row in results:
            self.ruleItemDict[row[0]] = str(row[1])

    def fieldTypeByRule(self, *kw):
        countryNum = kw[0] if kw[0] != None else ''
        sourceType = kw[1] if kw[1] != None else ''
        mediaSectionId = kw[2] if kw[2] != None else ''
        tags = kw[3] if kw[3] != None else ''
        mediaId = kw[4] if kw[4] != None else ''
        languageType = kw[5] if kw[5] != None else ''
        libraryClass = kw[6] if kw[6] != None else ''
        xhsClass = kw[7] if kw[7] != None else ''
        zyfwClass = kw[8] if kw[8] != None else ''
        keywords = kw[9] if kw[9] != None else ''
        if self.fieldRuleLists.__len__() != 0:
            for fieldRuleDict in self.fieldRuleLists:
                flag = 0
                fieldId = fieldRuleDict['fieldId']
                fieldRuleItemLists = fieldRuleDict['fieldRuleItem']
                for fieldRuleItemDict in fieldRuleItemLists:
                    contentRelation = fieldRuleItemDict['contentRelation']
                    itemName = fieldRuleItemDict['itemId']
                    contentLists = fieldRuleItemDict[itemName].strip().split(';')
                    if isinstance(locals()[itemName], str) is False:
                        itemContent = locals()[itemName].encode('utf-8', 'ignore')
                    else:
                        itemContent = locals()[itemName]
                    notEqflag = 1
                    for contentOftable in contentLists:
                        flag = 0
                        # contentOftable = contentOftable.encode('utf-8', 'ignore')
                        if contentRelation == '1' and contentOftable == itemContent:
                            flag = 1
                        elif contentRelation == '0' and contentOftable in itemContent:
                            notEqflag = 0
                            break
                        elif contentRelation == '2':
                            if contentOftable in itemContent:
                                flag = 1
                        if flag == 1:
                            break

                    if contentRelation == '0' and notEqflag == 1:
                        flag = 1
                    if flag == 0:
                        break

                if flag == 1:
                    return fieldId

        return

    def getDictOfMediaSection(self):
        mediaSectionSql = "select mediaSectionId,mediaId,languageNum,tags,countryNum,regionNum,mediaSectionName from hs_media_section where isDelete = '0'"
        results = self.sql_help.execute_sql(mediaSectionSql)
        for row in results:
            self.mediaByMediaSectionDict[row[0]] = row[1]
            self.languageNumByMediaSectionDict[row[0]] = row[2]
            self.tagsByMediaSectionDict[row[0]] = row[3]
            self.countryNumByMediaSectionDict[row[0]] = row[4]
            self.regionNumByMediaSectionDict[row[0]] = row[5]
            self.mediaSectionDict[row[0]] = '1'
            self.mediaSectionNameDict[row[6]] = '1'
            self.mediaSectionIdByNameDict[row[6]] = row[0]
            self.mediaSectionNameBySectionId[row[0]] = row[6]

    def getMediaSection(self, siteCofName):
        u"""
                获取国家码
        """
        mediaSectionSql = "select countryNum,languageNum from hs_media_section where mediaSectionName = '" + siteCofName + "'"
        execute_sql = self.sql_help.execute_sql(mediaSectionSql)
        for row in execute_sql:
            return row[0], row[1]

    def savePicOrFile(self, *kw):
        fileName = kw[0]
        basePath = kw[1]
        sTypePath = kw[2]
        str_time = time.strftime('%Y%m%d')
        year_path = os.path.join(basePath, str_time[:4]).replace('\\', '/')
        time_path = os.path.join(year_path, str_time).replace('\\', '/')
        lastPath = os.path.join(time_path, sTypePath).replace('\\', '/')
        self.__mkdir(year_path)
        self.__mkdir(time_path)
        self.__mkdir(lastPath)
        if self.__img:
            imgName = self.uuid + '.jpg'
            webPath = os.path.join(('/').join(fileName.split('/')[:-1]), self.__img).replace('\\', '/')
            absolutePath = os.path.join(lastPath, imgName).replace('\\', '/')
            try:
                shutil.copyfile(webPath, absolutePath)
                self.__localImgPath = os.path.join('/temp', str_time[:4], str_time, sTypePath, imgName).replace('\\','/')
            except FileNotFoundError as e:
                self.__localImgPath = None
        if self.__fileLists:
            attrLists = []
            for pdfFile in self.__fileLists:
                try:
                    pdfName = str(uuid.uuid1()).replace('-', '') + '.pdf'
                    webPath = os.path.join(('/').join(fileName.split('/')[:-1]), pdfFile).replace('\\', '/')
                    absolutePath = os.path.join(lastPath, pdfName).replace('\\', '/')
                    localFilePath = os.path.join('/temp', str_time[:4], str_time, sTypePath, pdfName).replace('\\',
                                                                                                              '/')
                    shutil.copyfile(webPath, absolutePath)
                    attrDict = {}
                    attrDict['fileName'] = pdfFile
                    attrDict['filePath'] = localFilePath
                    attrLists.append(attrDict)
                except Exception as e:
                    pass

            if len(attrLists) != 0:
                self.__attributeJson = json.dumps(attrLists, ensure_ascii=False)
        return

    def __mkdir(self, path):
        path = path.strip()
        path = path.rstrip('\\')
        isExists = os.path.exists(path)
        if not isExists:
            os.makedirs(path)
            return True
        return None
