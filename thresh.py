#!/usr/bin/env python

# Create
# monasca alarm-definition-create test "cpu.percent{hostname=ubuntu}>50}
# monasca alarm-definition-create test1 "cpu.percent{hostname=opensuse}>50}

# RUN:
# faust -A thresh worker -l info

# OBSERVE:
# kafkacat -C -b localhost -t prototyping-subexprs_states-changelog

# CREATE LOAD
#  dd if=/dev/urandom | bzip2 -9 >> /dev/null



import faust
import pymysql

from collections import defaultdict
from datetime import timedelta
from datetime import datetime as dt
from itertools import takewhile
import traceback
from typing import (Any,
                    Dict,
                    Optional)

#def infinite_dd():
#    return defaultdict(infinite_dd)

ALARM_SQL = "select * from alarm_definition where deleted_at is NULL order by created_at"
SUB_ALARM_SQL = """select sad.*, sadd.* from sub_alarm_definition sad 
    left outer join sub_alarm_definition_dimension sadd on sadd.sub_alarm_definition_id=sad.id 
    where sad.alarm_definition_id = '%s' order by sad.id"""

app = faust.App('prototyping', broker='kafka://localhost:9092')


class SmallMetricT(faust.Record, serializer='json'):
    timestamp: float
    value: float


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
mapto = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
als = dict()
subals = dict()
metric_topic = app.topic('metrics', value_type=Message)
QUEUE_SIZE = 300
measures_table = app.Table('measures', default=list, partitions=64)
subexprs_table = app.Table('subexprs_states', default=False, partitions=64)
FUNCTIONMAP = { 'SUM':sum, 'MAX':max, 'MIN':min }


@app.agent(metric_topic)
async def print_metrics(stream):
    global mapto
    async for event in stream.group_by(Message.meta.tenantId, partitions=64):
        try:
            recvd = set(event.metric.dimensions.items())
            needed = mapto[event.meta.tenantId].get(event.metric.name, {})
            for sid, dimset in needed.items():
                if dimset & recvd == dimset:
                    curr = SmallMetricT(timestamp=event.metric.timestamp/1000,
                                        value=event.metric.value)

                    values = measures_table[sid] 
                    values.insert(0, curr)
                    # ....................  <- measures
                    #    |take everything|
                    #  q_size           now <- timestamps
                    after = dt.now().timestamp() - QUEUE_SIZE
                    # take values whose have timestamp greater than (now-QUEUE_SIZE)
                    try:
                        values = list(takewhile(lambda m: after < m.timestamp, values))
                    except:
                        def loads(m):
                            if isinstance(m,dict) and "__faust" in m.keys():
                                return faust.Record.loads(m)
                            return m
                        values = list(map(loads, values))
                        values = list(takewhile(lambda m: after < m.timestamp, values))
                    measures_table[sid] = values

#                    vals = measures_table[sid]
#                    try:
#                        print(vals[0].timestamp - vals[-1].timestamp)
#                        print(len(vals))
#                    except:
#                        print(vals)
#                        raise
#                    print((existing))
                    evaluator(sid)
        except Exception as e:
            print('*'*80)
            traceback.print_exc()
            print('*'*80)

def threshold(operator, lhs, rhs):
    if operator == 'LT':
        return lhs < rhs
    elif operator == 'LTE':
        return lhs <= rhs
    elif operator == 'GT':
        return lhs > rhs
    elif operator == 'GTE':
        return lhs >= rhs
    else:
        return False


def getValuesTrimmedToPeriod(sbexpr):
    since = dt.now().timestamp() - sbexpr['period']
    values = takewhile(lambda m: since < m.timestamp, measures_table[sbexpr['id']])
    return map(lambda m: m.value, values)


def evaluator(sid):
    aid = subals[sid]
    sbexpr = als[aid]['subexpressions'][sid]
    values = getValuesTrimmedToPeriod(sbexpr)
    values = list(values)

    try:
        res = FUNCTIONMAP[sbexpr['function']](values)
    except Exception as e:
        traceback.print_exc()
        return
    state = threshold(sbexpr['operator'], res, sbexpr['threshold'])

    print(f'{aid}: {sbexpr["function"]}({values}) {sbexpr["operator"]} {sbexpr["threshold"]} {state}')
    subexprs_table[sbexpr['id']] = state


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
    global mapto, subals, als
    connection = pymysql.connect('127.0.0.1', 'root', 'secretdatabase', 'mon')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(ALARM_SQL)
    alarms = cursor.fetchall()
    
    for al in alarms:
        cursor.execute(SUB_ALARM_SQL % al['id'])
        aid = al['id']
        als[aid] = al
        als[aid]['subexpressions'] = {}
        sbexprs = cursor.fetchall()
        for sbexpr in sbexprs:
            sid = sbexpr['id']
            als[aid]['subexpressions'][sid] = sbexpr
            subals[sid] = aid
            # metric A is needed with dimensions (B=C) and (D=E)
            # so make a set {(B,C),(D=E)} for each sbexpr.
            # For better perf this mapping can be reversed into a dict,
            # dict[metric.name][frozenset {(B,C),(D=E)}] = [sid]
            # dict[metric.name][frozenset {(B,X),(D=Y)}] = [sid]
            sbalm2dimset = mapto[al['tenant_id']][sbexpr['metric_name']]
            if sbexpr['dimension_name']:
                dim = (sbexpr['dimension_name'],sbexpr['value'])
            # sub_alarm_definition_id is derived from
            # sub_alarm_definition_dimension table, which might not have
            # an entry for some subexpression if there is no
            # dimension defined on the metric
                sbalm2dimset[sid].add(dim)
            else:
                # metric without dimensions
                _ = sbalm2dimset[sid]
                # just get the id so an empty set is added
                # set() & set(<anything>) == set() 
            

if __name__ == '__main__':
    app.main()
