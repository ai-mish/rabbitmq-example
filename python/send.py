#!/usr/bin/env python
import pika


credentials = pika.PlainCredentials('sasdemo', 'Orion123')
parameters = pika.ConnectionParameters('192.168.56.111',
                                       5672,
                                       '/',
                                       credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='sas.audit.queue',durable=True)

channel.basic_publish(exchange='',
                      routing_key='sas.audit.queue',
                      body='Hello World!')
print(" [x] Sent 'Hello World!'")

connection.close()
