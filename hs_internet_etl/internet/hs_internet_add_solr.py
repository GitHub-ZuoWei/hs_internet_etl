import uuid

import pysolr
import pymysql

db = pymysql.connect('192.168.10.237', user='root', password='123456', db='hsopentemporary')
conn_cursor = db.cursor()

solr_core_admin = pysolr.Solr("http://192.168.10.237:8090/solr/new_core")


def solrInfoDict(sorlTuple):
    solrDict = {}
    solrDict['id'] = sorlTuple[0]
    solrDict['type'] = sorlTuple[1]
    solrDict['title'] = sorlTuple[2]
    solrDict['mediaSectionId'] = sorlTuple[3]
    solrDict['fieldType'] = sorlTuple[4]
    solrDict['countryType'] = sorlTuple[5]
    solrDict['languageType'] = sorlTuple[6]
    solrDict['sourceType'] = sorlTuple[7]
    solrDict['coverPhoto'] = sorlTuple[8]
    solrDict['tableCode'] = sorlTuple[9]
    solrDict['oriTime'] = str(sorlTuple[10])
    solrDict['sortTime'] = str(sorlTuple[11])
    solrDict['inTime'] = str(sorlTuple[12])
    solrDict['newsContent'] = str(sorlTuple[13])
    solrDict['downloadLinks'] = sorlTuple[14]
    return solrDict


for page in range(1, 2):
    conn_cursor.execute(
        "select a.id id,a.type, a.title, a.mediaSectionId,a.fieldType,a.countryType,a.languageType, a.sourceType,a.coverPhoto,a.tableCode,a.oriTime,a.oriTime sortTime,a.inTime,b.content newsContent,b.downloadLinks from hs_data_jor a RIGHT join hs_news_2000 b on a.id = b.id WHERE inTime BETWEEN '2020-04-20' AND '2020-04-26'  limit %s,%s",
        ((page - 1) * 1000, 1000))
    fetchall = conn_cursor.fetchall()
    for item in fetchall:
        info_dict = solrInfoDict(item)
        print(info_dict)
        solr_core_admin.add([info_dict], commit=True)
#         solr_core_admin.add([{'id': 1, 'type': '1', 'title': '这场阻击战，TA们顶在一线！',
#                               'mediaSectionId': '4434b29c40ab46b796c64062142e2c32', 'fieldType': 'null',
#                               'countryType': '1224', 'languageType': '38', 'sourceType': '4000', 'coverPhoto': 'null',
#                               'tableCode': 0, 'oriTime': '2020-01-31 10:33:05', 'sortTime': '2020-01-31 10:33:05',
#                               'inTime': '2020-04-25 21:40:35',
#                               'newsContent': '<p></p><p> </p> <p></p><p> </p><p></p><p> </p> <p></p><p> </p><p></p><p> </p><p></p> </p> </p><p> </p> ▌危难时刻，党员干部要挺身而出——论坚决打赢疫情防控阻击战<p> </p> “全面贯彻坚定信心、同舟共济、科学防治、精准施策的要求，让党旗在防控疫情斗争第一线高高飘扬。”日前，习近平总书记作出重要指示，要求各级党组织和广大党员干部坚决贯彻落实党中央决策部署，紧紧依靠人民群众坚决打赢疫情防控阻击战。把思想和行动统一到习近平总书记重要指示精神和党中央决策部署上来，团结带领广大人民群众扎实做好疫情防控各项工作，是全党上下当前的一项重大政治任务。</p> </p> 关键时刻，关键在党。面对来势汹汹的疫情，做好防控工作必须加强党的领导。各级党委（党组）要科学判断形势、精准把握疫情，统一领导、统一指挥、统一行动，把党的政治优势、组织优势、密切联系群众优势转化为疫情防控的强大政治优势。要坚持疫情防控一盘棋，形成各部门各单位既各司其职又密切配合的防控工作格局，最大限度凝聚防控合力，为打赢疫情防控阻击战提供有力保障。</p> </p> 疫情防控是没有硝烟的战场，也是检验党员、干部初心使命的考场。人民利益高于一切，情况越是危急、任务越是紧迫，越需要发扬不畏艰险、无私奉献的精神，越需要弘扬从严从实、狠抓落实的优良作风，越需要广大党员、干部当好群众的贴心人和主心骨，为疫情防控注入强大正能量。</p> </p> 大事难事见担当，危难时刻显本色。打仗即打将，各级党组织领导班子和领导干部特别是主要负责同志要坚守岗位、靠前指挥，做到守土有责、守土担责、守土尽责。领导干部把投身防控疫情第一线作为践行初心使命、体现责任担当的试金石和磨刀石，充分掌握防控工作实际情况，及时发现不足与短板，迅速采取有效措施改进，才能不断提升防控工作的针对性、实效性，为广大人民群众筑牢健康防线。</p> </p> 一个支部就是一座堡垒，一名党员就是一面旗帜。防控疫情，还有不少难关要闯，有不少硬骨头要啃，需要充分发挥基层党组织的战斗堡垒作用、广大党员的先锋模范作用。把责任扛起来，把标杆立起来，做到哪里任务险重哪里就有党组织坚强有力的工作、哪里就有党员当先锋作表率，广泛动员群众、有效组织群众、团结凝聚群众，全面落实联防联控措施，构筑起群防群治的严密防线。</p> </p> 疾风知劲草，烈火炼真金。各级党委（党组）要在疫情防控第一线考察、识别、评价、使用干部，把领导班子和领导干部在疫情防控斗争中的实际表现作为考察其政治素质、宗旨意识、全局观念、驾驭能力、担当精神的重要内容。对表现突出的，要表扬表彰、大胆使用；对不敢担当、作风飘浮、落实不力的，要严肃问责。树立鼓励担当、崇尚实干的用人导向，激励引导广大党员、干部在危难时刻挺身而出、英勇奋斗、扎实工作，交出无愧于党和人民的优异答卷。（新华社评论员）</p><p> </p> 往期精选\xa0 \xa0\xa0\xa0\xa0\xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0 \xa0\xa0 </p> 习近平：紧紧依靠人民群众坚决打赢疫情防控阻击战</p> 党旗，高高飘扬在防控疫情斗争第一线 </p> 中组部：要注重在疫情防控阻击战一线考察识别领导班子和领导干部 </p> </p> 来源：新华社</p> 责编：姚润萍</p> 编审：翟子赫</p> </p>',
#                               'downloadLinks': 'http://mp.weixin.qq.com/s?__biz=MzA4ODUzNjAxOA==&mid=2671092734&idx=2&sn=d5bd80396029deacd68da161e3bb1440&chksm=8ae9a543bd9e2c55a0e9fb0ad7460b3a2acba9e00574a3f5eadf6c4002d382df6d6bf14d14f0#rd'}
#                              ], commit=True)


# solr_core_admin.add([{
#     'id': str(uuid.uuid1()).replace('-', ''),
#     "content": "testest"
# }], commit=True)
# solr_core_admin.delete(id='zwtest')
# solr_core_admin.commit()
# solr_core_admin.add([{
#     "id": 'zwtest',
#     "name": "6666"
# }], commit=True)

# search = solr_core_admin.search('id:zwtest')
# for item in search:
#     print(item)
