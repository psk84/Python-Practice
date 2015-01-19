import sys
import pymongo

# file insert
#f = open("C:/Dev/test.txt", 'w')

#for i in range(1, 11): 
#    data = "[%d send]success.\n" % i 
#    f.write(data)
     
#for j in range(12, 23): 
#    data = "[%d send]fail.\n" % j 
#    f.write(data)
#f.close()

success_count = 0;
fail_count = 0;

f = open("C:/Dev/test.txt", 'r') 
while 1: 
    line = f.readline()
    if not line: break 
    print(line)
    if "success" in line:
    
        success_count = success_count + 1
    else :
        fail_count = fail_count + 1
f.close()

print("success=%d" % success_count)
print("fail=%d" % fail_count)


# mongo db insert
connection = pymongo.MongoClient("mongodb://localhost")
db = connection.terrydb
users = db.users

# db data delete
users.remove()

doc = {'_id':'myid', 'successCount': success_count,'failCount': fail_count}
 
try:
    users.insert(doc)
except:
     print("fail") 