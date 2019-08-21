#!/usr/bin/env python
import sys
import faust
import pymysql
from collections import defaultdict
from datetime import timedelta
from typing import (
        Any,
        Dict,
        Optional
        )

#def infinite_dd():
#    return defaultdict(infinite_dd)

ALARM_SQL = "select * from alarm_definition where deleted_at is NULL order by created_at"
SUB_ALARM_SQL = """select sad.*, sadd.* from sub_alarm_definition sad 
    left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id 
    where sad.alarm_definition_id = '%s' order by sad.id"""

app = faust.App('prototyping', broker='kafka://localhost:9092')


class MetricT(faust.Record, serializer='json'):
    name: str
    value_meta: Any
    dimensions: Dict[str, str]
    value: float
    timestamp: float


class MetricMetaT(faust.Record, serializer='json'):
    region: str
    tenantId: str


class Message(faust.Record, serializer='json'):
    creation_time: int
    meta: MetricMetaT
    metric: MetricT


# mapto[tenantid][metricname][subalrmid]
# = set(
mapto = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
subalmsmap = dict() #mapping from subalarmid to alarmname
metric_topic = app.topic('metrics', value_type=Message)
WINDOW_SIZE = 10
WINDOW_STEP = 5
tenantsTable = app.Table('hugeTable', default=list).hopping(WINDOW_SIZE, WINDOW_STEP, expires=timedelta(minutes=10))

@app.agent(metric_topic)
async def print_metrics(stream):
    global mapto
    async for event in stream.group_by(Message.meta.tenantId):
        try:
#            decoded_message = json.loads(event.message.value)
            metric = event.metric
            dimensions = metric.dimensions
            recvd = set(dimensions.items())
            needed = mapto[event.meta.tenantId].get(metric.name, {})
            fitting = False
            for subalm, dimset in needed.items():
                if dimset & recvd == dimset:
                    tenantsTable[subalm] += [event.metric]
                    alname = subalmsmap[subalm]
                    print(f'need {event.metric.name} with {event.metric.dimensions} from {dimset} for {alname} {subalm}')
                    fitting = True
            if not fitting and needed :
                print(f'{needed.values()} not in {recvd} with {event.metric.name}')
        except Exception as e:
            print('*'*80)
            print(e)
            raise e
            print('*'*80)


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
    global mapto
    connection = pymysql.connect('127.0.0.1', 'root', 'secretdatabase', 'mon')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(ALARM_SQL)
    

    for al in cursor.fetchall():
        #mapto[al['tenant_id']] = defaultdict()
        al['seen'] = False
        cursor.execute(SUB_ALARM_SQL % al['id'])
        al['subexpression'] = cursor.fetchall()
        for subexpr in al['subexpression']:
            subalms = mapto[al['tenant_id']][subexpr['metric_name']]
            if subexpr['dimension_name']:
                dim = (subexpr['dimension_name'],subexpr['value'])
                subalms[subexpr['id']].add(dim)
            else:
                # metric without dimensions
                _ = subalms[subexpr['id']]
                # just get the id so an empty set is added
                # set() & set(<anything>) == set() 
            # sub_alarm_definition_id is derived from sub_alarm_definition_dimension table, which might not have
            # an entry for some subexpression if there
            # is no dimension defined on the metric
            subalmsmap[subexpr['id']] = al['name']
            
            #mapto[al['tenant_id']][subexpr['metric_name']].update(subalms)

    for i in mapto[al['tenant_id']]:
        print(i)
        print(mapto[al['tenant_id']][i])
        
            

if __name__ == '__main__':
    app.main()
