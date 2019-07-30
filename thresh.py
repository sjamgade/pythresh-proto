#!/usr/bin/env python
import json
import faust
import pymysql
from collections import defaultdict
from datetime import timedelta

ALARM_SQL = "select * from alarm_definition where deleted_at is NULL order by created_at"
SUB_ALARM_SQL = """select sad.*, sadd.* from sub_alarm_definition sad 
    left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id 
    where sad.alarm_definition_id = '%s' order by sad.id"""

app = faust.App('prototyping', broker='kafka://localhost:9092')

metric_topic = app.topic('metrics')


tables = dict()
alarms = []
mapto = defaultdict(list)

@app.agent(metric_topic)
async def print_metrics(stream):
    global mapto
    async for event in stream.events():
        try:
            decoded_message = json.loads(event.message.value)
            metric = decoded_message['metric']
            metric_name = metric['name']
            dimensions = metric.get('dimensions', {})
            for dim in dimensions.items():
                als = mapto.get((metric_name, dim), None)
                if als:
                    await event.forward(handle_alarm_metrics)
        except Exception as e:
            print('*'*80)
            print(e)
            print('*'*80)


@app.agent()
async def handle_alarm_metrics(metrics):
    async for msg in metrics:
        print(msg)

@app.agent(app.topic('events'))
async def handle_alarm_definitions(stream):
    async for msg in stream:
        if 'alarm-definition-created' in msg.keys():
            pass
        elif 'alarm-definition-deleted' in msg.keys():
            pass
        elif 'alarm-definition-updated' in msg.keys():
            pass


@app.task
async def create_infra():
    global alarms, mapto
    connection = pymysql.connect('127.0.0.1', 'root', 'secretdatabase', 'mon')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(ALARM_SQL)
    alarms += cursor.fetchall()

    for al in alarms:
        al['seen'] = False
        cursor.execute(SUB_ALARM_SQL % al['id'])
        al['subexpression'] = cursor.fetchall()
        for subexpr in al['subexpression']:
            hsh = (subexpr['metric_name'], (subexpr['dimension_name'],subexpr['value']))
            mapto[hsh] += al
            

if __name__ == '__main__':
    app.main()
