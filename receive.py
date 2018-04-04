#!/usr/bin/env python
import pika


credentials = pika.PlainCredentials('sasdemo', 'Orion123')
parameters = pika.ConnectionParameters('192.168.56.111',
                                       5672,
                                       '/',
                                       credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

connection.close()
