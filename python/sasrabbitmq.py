#!/usr/bin/env python

__author__ = "mudit.mishra@sas.com"
__copyright__ = "Copyright (C) 2018 mudit.mishra@sas.com"
__license__ = "Public Domain"
__version__ = "1.0"

#Standar libraries
import logging
import sys
import time
import json
import io
import argparse

#Pika module to consume messages from AMQP or RabbitMQ
import pika


# Make it work for Python 2+3 and with Unicode

try:
    to_unicode = unicode
except NameError:
    to_unicode = str

LOG_FORMAT = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)


class RMQConsumer(object):

    def __init__(self, amqp_url):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._queuename = 'tripwireapplication.#'
        self._queue_state = None
        self._queue_size = 0
        self._messages_fetch_limit = 2
        self._message_consume_counter = 0
        self._messages_dict = []
        self._consumer_tag = None

    def connect(self):
        LOGGER = logging.getLogger('RMQConsumer.connect')
        LOGGER.info('Connecting to %s', self._url)
        parameters = pika.URLParameters(self._url)
        try:
            self._connection = pika.BlockingConnection(parameters)
        except Exception as e:
            LOGGER.error(e)

    def open_channel(self):
        LOGGER = logging.getLogger('RMQConsumer.open_channel')
        if self._connection:
            self._channel = self._connection.channel()
            LOGGER.info('Channel opened')

    def setup_queue(self):
        LOGGER = logging.getLogger('RMQConsumer.setup_queue')
        if self._channel:
            LOGGER.info('Declaring queue %s', self._queuename)
            self._queue_state = self._channel.queue_declare(queue=self._queuename,durable=True)
            LOGGER.info('Getting queue size')
            if self._queue_state:
                self._queue_size = self._queue_state.method.message_count
            else:
                self._queue_size = 0
            LOGGER.info('Queue %s Size %d' % (self._queuename,self._queue_size))

    def getFetchLimit(self):
        LOGGER = logging.getLogger('RMQConsumer.getFetchLimit')
        if self._queue_size:
            if self._queue_size < self._messages_fetch_limit:
                messages_fetch_limit = self._queue_size
            else:
                messages_fetch_limit = self._messages_fetch_limit
                LOGGER.info("Queue size %d is great then message count %d" % (self._queue_size,messages_fetch_limit))
                LOGGER.info("Message to read per consumer: %d" % (messages_fetch_limit))
        else:
            messages_fetch_limit = 0

        return messages_fetch_limit

    def start_consume(self):
        LOGGER = logging.getLogger('RMQConsumer.start_consume')
        try:
            prefetch_limit=self.getFetchLimit()
            if prefetch_limit:
                self._channel.basic_qos(prefetch_count=self.getFetchLimit())
                self._consumer_tag = self._channel.basic_consume(self.on_message,
                                  queue=self._queuename)
                self._channel.start_consuming()
            else:
                self.close_connection()
                raise SystemExit
        except Exception as err:
            LOGGER.error(err)
            self.close_connection()

        except KeyboardInterrupt:
            self.close_connection()

        self.stop_consuming()
        self.close_connection()


    def close_channel(self):
        LOGGER = logging.getLogger('RMQConsumer.close_channel')
        LOGGER.info('Closing the channel')
        self._channel.close()

    def stop_consuming(self):
        LOGGER = logging.getLogger('RMQConsumer.stop_consuming')
        if self._channel:
            self._channel.stop_consuming()
            LOGGER.info('Consumer Stopped')

    def close_connection(self):
        LOGGER = logging.getLogger('RMQConsumer.close_connection')
        if self._connection:
            self._connection.close()
            LOGGER.info('Connection Closed')


    def on_message(self,ch, method, properties, body):
        LOGGER = logging.getLogger('RMQConsumer.on_message')
        self._message_consume_counter += 1
        self._messages_dict.append(json.loads(body))
        if self._message_consume_counter == self._messages_fetch_limit:
            self.stop_consuming()
            self.close_connection()

    def save_messages(self):
        LOGGER = logging.getLogger('RMQConsumer.save_messages')
        timestr = time.strftime("%Y%m%d_%H%M%S")
        with open('messages_%s.json' % (timestr), 'w') as outfile:
            json.dump(self._messages_dict, outfile, indent = 4)

    def run(self):
        self._message_consume_counter = 0
        self.connect()
        self.open_channel()
        self.setup_queue()
        self.start_consume()
        self.save_messages()


def run(action, amqp_uri, queue, filename):
    LOGGER = logging.getLogger('run')
    #consumer = RMQConsumer('amqp://guest:guest@pdcesx16063.exnet.sas.com:5672/%2F')
    try:
        consumer.run(action, amqp_uri, queue, filename)
    except KeyboardInterrupt:
        #consumer.stop()
        LOGGER.info('Stop')


def main(argv):
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    LOGGER = logging.getLogger('sasrabbitmq')

    #Command line options parser and validation
    parser = argparse.ArgumentParser(prog='sasrabbitmq.py', usage='%(prog)s [options]')
    requiredNamed = parser.add_argument_group('Required arguments')
    requiredNamed.add_argument('-receive',
                               action="store_true",
                               help='Receive message from RMQ without acknowledgement')
    requiredNamed.add_argument('-ack',
                               action="store_true",
                               help='Acknowledge message from RMQ without acknowledgement')
    requiredNamed.add_argument('-u', '--uri',
                               dest='uri',
                               help="URI needed by pikka module to connect to RabbitMQ server e.g. amqp://username:password@rabbitmq.mydomain.com:5672/%%2F",
                               type=str,
                               required=True)
    requiredNamed.add_argument('-q', '--queue',
                               dest='queue',
                               help='Name of RabbitMQ queue',
                               type=str,
                               required=True)
    requiredNamed.add_argument('-o', '--output','-i', '--input',
                                dest='file',
                                help='Output or Input file where messages are stored',
                                required=True,
                                type=str
                                )
    #parser.add_argument('--amqp_uri', dest='accumulate', action='store_const',
    #               const=amqp_uri, default="amqp://guest:guest@plocalhost:5672/%2F",
    #               help='URI needed by pikka module to connect to RabbitMQ server.')

    #results = parser.parse_args(['receive', '-u', 'amqp://' '-o', '/tmp/temp.txt'])
    results = parser.parse_args()

    if results.receive:
        action='receive'
    elif results.ack:
        action='ack'
    else:
        action='NA'

    amqp_uri = results.uri
    filename = results.file
    queue = results.queue

    try:
        #consumer.run(action, amqp_uri, queue, filename)
        LOGGER.info("Action: %s" % action)
        LOGGER.info("URI: %s" % amqp_uri)
        LOGGER.info("Queue: %s" % queue)
        LOGGER.info("Filename: %s" % filename)
    except KeyboardInterrupt:
        #consumer.stop()
        LOGGER.info('Stop')

    print(results)

    return 0

if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[0:]))
    except:
        # Try our best to log the error.
        try:
            log.exception("Uncaught error running Python code to process RabbitMQ messages")
        except Exception:
            pass
        raise
