#!/usr/bin/env python
import json
import faust
import pymysql
import asyncio
from collections import defaultdict
from datetime import timedelta, datetime

from faust import Sensor

ALARM_SQL = "select * from alarm_definition where deleted_at is NULL order by created_at"
SUB_ALARM_SQL = """select sad.*, sadd.* from sub_alarm_definition sad 
    left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id 
    where sad.alarm_definition_id = '%s' order by sad.id"""


app = faust.App('prototyping', broker='kafka://localhost:9092')

# this sensor can be used for waiting to begin processing again 
# and not skip at all
class RebalanceSensor(Sensor):
    def __init__(self, *args,**kwargs):
        super().__init__(*args,**kwargs)
        self.rebstart = asyncio.Event()
        self.rebend = asyncio.Event()
        self.waiting_tables = dict()

    def on_rebalance_start(self, *args, **kwargs):
        self.rebstart.set()
        self.rebstart.clear()

    def on_rebalance_end(self, state):
        pass



#rebalance_sensor = RebalanceSensor()
#app.sensors.add(rebalance_sensor)

metric_topic = app.topic('metrics')


tables = dict()
mapto = defaultdict(list)
table_creation_time = dict()


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
                al = mapto.get((metric_name, dim), None)
                if al:
                    key = f'{metric_name}-{dim[0]}-{dim[1]}'.encode()
                    value = metric
                    if key not in tables.keys():
#                        rebalance_sensor.rebstart.clear()
#                        rebalance_sensor.state = ( key, app.Table(key.decode(), default=list, partitions=64).hopping(60, 5, expires=timedelta(minutes=10)) )
                        table_creation_time[key] = datetime.now()
                        tables[key] = app.Table(key.decode(), default=list, partitions=64).hopping(60, 5, expires=timedelta(minutes=10))
                        app.consumer._thread.request_rejoin()
                        #await rebalance_sensor.rebstart.wait()
                        #await rebalance_sensor.rebend.wait()
                        print(f'{key} rebalance end')
                        #await app.topics.wait_for_subscriptions()
                    if datetime.now() > (table_creation_time[key] + timedelta(seconds=5)):
                        tables[key][value['name']] += [value['value']]
                    else:
                        print(f'skipping {key}')
        except Exception as e:
            print('*'*80)
            print(str(e))
            print('*'*80)

#@app.agent()
#async def handle_table_creation(measurements):
#    async for key,value in measurements.items():


#@app.agent()
#async def handle_alarm_metrics(metrics):
#    async for metric in metrics:
#        pass

@app.agent(app.topic('events'))
async def handle_alarm_definitions(stream):
    async for msg in stream:
        if 'alarm-definition-created' in msg.keys():
            print(msg.values)
        elif 'alarm-definition-deleted' in msg.keys():
            print(msg.values)
        elif 'alarm-definition-updated' in msg.keys():
            print(msg.values)

#@app.timer(2.0)
#async def dumpinfo():
#    for key in tables:
#        print(tables[key]['cpu.percent'].delta(10))

@app.task
async def create_infra():
    global mapto
    connection = pymysql.connect('127.0.0.1', 'root', 'secretdatabase', 'mon')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(ALARM_SQL)

    for al in cursor.fetchall():
        al['seen'] = False
        cursor.execute(SUB_ALARM_SQL % al['id'])
        al['subexpression'] = cursor.fetchall()
        for subexpr in al['subexpression']:
            hsh = (subexpr['metric_name'], (subexpr['dimension_name'],subexpr['value']))
            mapto[hsh] += al
            
        #await al['topic'].maybe_declare()
#            WINDOW_SIZE = subexpr['period'] * subexpr['periods']
#            WINDOW_SIZE = 1
#            metric_table = app.Table(
#                subexpr['metric_name'],
#                default=list
#            ).hopping(WINDOW_SIZE, WINDOW_STEP, expires=timedelta(minutes=10))
#            tables[metric_name] = metric_table

if __name__ == '__main__':
    app.main()
