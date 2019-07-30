import pymysql
from pprint import pprint

SUB_ALARM_SQL = """select sad.*, sadd.* from sub_alarm_definition sad 
    left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id 
    where sad.alarm_definition_id = '%s' order by sad.id"""


connection = pymysql.connect('127.0.0.1', 'root', 'secretdatabase','mon')
cursor = connection.cursor(pymysql.cursors.DictCursor)
cursor.execute("select * from alarm_definition where deleted_at is NULL order by created_at")
alarms=cursor.fetchall()


for al in alarms:
    cursor.execute(SUB_ALARM_SQL % al['id'])
    al['subexpression']=cursor.fetchall()
    pprint(al['subexpression'])
