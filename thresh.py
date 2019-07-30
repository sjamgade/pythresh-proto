#!/usr/bin/env python

# In this exapmple we have a function `publish_every_2secs` publishing a
# message every 2 senconds to topic `hopping_topic`
# We have created an agent `print_windowed_events` consuming events from
# `hopping_topic` that mutates the windowed table `values_table`

# `values_table` is a table with hopping (overlaping) windows. Each of
# its windows is 10 seconds of duration, and we create a new window every 5
# seconds.
# |----------|
#       |-----------|
#             |-----------|
#                   |-----------|

from random import random
from datetime import timedelta
import time
import faust
import statistics
from types import SimpleNamespace
from monasca_persister.repositories.utils import parse_measurement_message

app = faust.App('prototyping', broker='kafka://localhost:9092')

#metric_topic = app.topic('metrics', value_serializer='raw', key_type=str)
metric_topic = app.topic('metrics')





@app.agent(metric_topic)
async def print_metrics(stream):
    async for msg in stream:
        try:
            decoded_message = msg
            metric = decoded_message['metric']
            metric_name = metric['name']
            region = decoded_message['meta']['region']
            tenant_id = decoded_message['meta']['tenantId']
            time_stamp = metric['timestamp']
            value = float(metric['value'])
            value_meta = metric.get('value_meta', {})
            value_meta = {} if value_meta is None else value_meta
            dimensions = metric.get('dimensions', {})

            print(f"{time.ctime(time_stamp/1000)} {metric_name}, {dimensions}")
        except Exception as e:
            print('*'*80)
            print(msg)
            print('*'*80)
    


#TOPIC = 'hopping_topic'
#WINDOW_SIZE = 10
#WINDOW_STEP = 5
#
#hopping_topic = app.topic(TOPIC, value_type=Model)
#values_table = app.Table(
#    'values_table',
#    default=list
#).hopping(WINDOW_SIZE, WINDOW_STEP, expires=timedelta(minutes=10))


#@app.agent(hopping_topic)
#async def print_windowed_events(stream):
#    async for event in stream:  # noqa
#        values_table['values'] += [event.random]
#        values = values_table['values'].delta(WINDOW_SIZE)
#        print(f'-- New Event (every 2 secs) written to hopping(10, 5) --')
#        print(f'COUNT should start at 0 and after 10 secs be 5: '
#              f'{len(values)}')
#        print(f'SUM   should have values between 0-5: '
#              f'{sum(values) if values else 0}')
#        print(f'AVG   should have values between 0-1: '
#              f'{statistics.mean(values) if values else 0}')
#        print(f'LAST  should have values between 0-1: '
#              f'{event.random}')
#        print(f'MAX   should have values between 0-1: '
#              f'{max(values) if values else 0}')
#        print(f'MIN   should have values between 0-1: '
#              f'{min(values) if values else 0}')

#counter=0
#@app.timer(2.0, on_leader=True)
#async def publish_every_2secs():
#    global counter
#    counter+=1
#    msg = f'counter {counter}'
#    await metric_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
