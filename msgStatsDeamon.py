import json
import pymongo
from mercurial.templater import if_
from matplotlib.testing.jpl_units import day
from calendar import month

#DB 커넥션 설정
connection = pymongo.MongoClient("54.92.99.135", 27017)

db = connection.smartpush
collection  = db.testCollection
# aa = collection.find()

#write된 통계파일 읽기

f = open("파일 이름",'r')

while 1: 
    line = f.readline()
    if not line: break 
#파일에서 읽은 데이터 추출
    data = json.load(line)
    msgTag = data['snapshot']['msgTag']
    pushType = data['snapshot']['msgTag']
    status = data['snapshot']['status']
    pushFormat = data['snapshot']['pushFormat']
    sendType = data['snapshot']['sendType']
    created = data['snapshot']['sendType']
    serviceSeq = data['snapshot']['serviceSeq']
    userSeq = data['snapshot']['userSeq']
    year = created[0:4]
    month = created[5:8]
    day = created[9:12]
    hour = created[13:16]
    
#db에 데이터 insert

    #msgTag별 insert
    if status=="delivered" and pushType=="apns":
        apnsSuccessCnt = 1
    elif status=="delivered" and pushType=="gcm":
        gmcSuccessCnt = 1
    elif status!="delivered" and pushType=="apns":
        apnsFailCnt = 1
    elif status!="delivered" and pushType=="gcm":
        gcmFailCnt = 1
    
    collection.insert({"msgTag":msgTag,
                   "userSeq":userSeq,
                   "serviceSeq":serviceSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month,
                   "day":day,
                   "hour":hour})

    #userSeq별 year
    if status=="delivered" and pushType=="apns":
        apnsSuccessCnt = 1
    elif status=="delivered" and pushType=="gcm":
        gmcSuccessCnt = 1
    elif status!="delivered" and pushType=="apns":
        apnsFailCnt = 1
    elif status!="delivered" and pushType=="gcm":
        gcmFailCnt = 1
    
    collection.insert({"msgTag":msgTag,
                   "userSeq":userSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year})
    
    #userSeq별 month
    if status=="delivered" and pushType=="apns":
        apnsSuccessCnt = 1
    elif status=="delivered" and pushType=="gcm":
        gmcSuccessCnt = 1
    elif status!="delivered" and pushType=="apns":
        apnsFailCnt = 1
    elif status!="delivered" and pushType=="gcm":
        gcmFailCnt = 1
    
    collection.insert({"msgTag":msgTag,
                   "userSeq":userSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":"year",
                   "month":month})
        
    #userSeq별 day
    if status=="delivered" and pushType=="apns":
        apnsSuccessCnt = 1
    elif status=="delivered" and pushType=="gcm":
        gmcSuccessCnt = 1
    elif status!="delivered" and pushType=="apns":
        apnsFailCnt = 1
    elif status!="delivered" and pushType=="gcm":
        gcmFailCnt = 1
    
    collection.insert({"msgTag":msgTag,
                   "userSeq":userSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month,
                   "day":day})
    
    #userSeq별 hour
    collection.insert({"msgTag":msgTag,
                   "userSeq":userSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month,
                   "day":day,
                   "hour":hour})
    
    #serviceSeq별 year
    collection.insert({"serviceSeq":serviceSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year})
        
    #serviceSeq별 month   
    collection.insert({"serviceSeq":serviceSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month})
        
    #serviceSeq별 year,month,day
    collection.insert({"serviceSeq":serviceSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month,
                   "day":day})
    
    #serviceSeq별 year,month,day,hour
    collection.insert({"serviceSeq":serviceSeq,
                   "pushType":pushType,
                   "pushFormat":pushFormat,
                   "sendType":sendType,
                   "apnsSuccessCnt":apnsSuccessCnt,
                   "apnsFailCnt":apnsFailCnt,
                   "gcmSuccessCnt":gmcSuccessCnt,
                   "gcmFailCnt":gcmFailCnt,
                   "year":year,
                   "month":month,
                   "day":day,
                   "hour":hour})
f.close()