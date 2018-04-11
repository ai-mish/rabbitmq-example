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

MESSAGE_FILE_PREFIX = "message"
LOG_FORMAT = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)



class RMQConsumer(object):

    def __init__(self, amqp_uri,action,queue,filename):

        LOGGER = logging.getLogger('RMQConsumer.initialize')
        self._url = amqp_uri
        self._action = action
        self._queuename = queue
        self._ack = False

        self.filename = filename
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._queue_state = None
        self._queue_size = 0
        self._messages_fetch_limit = 2
        self._message_consume_counter = 0
        self._messages_dict = []
        self._consumer_tag = None
        self._localInMemoryMessageIds = []

        if action.lower() == 'delete':
            #Remove Messages in RabbitMQ by consume and acknowledgement
            self._ack = True
            LOGGER.info("Loading Messages ids from Text File %s" % filename)
            self._localInMemoryMessageIds = self.loadMsgIdFromTextFile(filename)

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
                if not _ack:
                    self._consumer_tag = self._channel.basic_consume(self.on_message,queue=self._queuename)
                else:
                    self._consumer_tag = self._channel.basic_consume(self.on_message,queue=self._queuename)
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
        message_body = json.loads(body)
        if not _ack:
            self._message_consume_counter += 1
            self._messages_dict.append(message_body)
            if self._message_consume_counter == self._messages_fetch_limit:
                self.stop_consuming()
                self.close_connection()
        else:
            id = message_body['id']
            if self.checkMessageExist(id):
                self.acknowledge_message(method.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def checkMessageExist(self,id):
        if id in self._localInMemoryMessageIds:
            return True
        else:
            return False

    def save_messages(self):
        LOGGER = logging.getLogger('RMQConsumer.save_messages')
        timestr = time.strftime("%Y%m%d_%H%M%S")
        with open('messages_%s.json' % (timestr), 'w') as outfile:
            json.dump(self._messages_dict, outfile, indent = 4)

    def loadMsgIdFromTextFile(self,in_text_file):
        LOGGER = logging.getLogger('Consumer.loadLocalMessageIds')
        localInMemoryMessageIds = []
        try:
            for line in in_text_file.readlines():
                LOGGER.debug("Acknowledging Message %s" % line)
                localInMemoryMessageIds.append(line)
        except Exception as err:
            LOGGER.error(err)
            localInMemoryMessageIds = []

        return localInMemoryMessageIds

    def run(self,action, queue, filename):
        LOGGER = logging.getLogger('RMQConsumer.run')
        self._message_consume_counter = 0
        if action.lower() == 'delete':
            LOGGER.info("To remove message from RabbitMQ consume and Acknowledge messages")
            self._ack = True
            LOGGER.info("Loading Messages ids from Text File %s" % filename)
            self.loadMsgIdFromTextFile(filename)
        self.connect()
        self.open_channel()
        self.setup_queue()
        self.start_consume()
        self.save_messages()


def getArguments():
        LOGGER = logging.getLogger('getInputParameters')
        parser = argparse.ArgumentParser(prog='sasrabbitmq.py', usage='%(prog)s [options]')
        requiredNamed = parser.add_argument_group('Required arguments')
        requiredNamed.add_argument('-receive',action="store_true",help='Receive message from RMQ without acknowledgement')
        requiredNamed.add_argument('-delete',action="store_true", help='Acknowledge message from RMQ without acknowledgement')
        requiredNamed.add_argument('-u', '--uri',dest='uri',help="URI needed by pikka module to connect to RabbitMQ server e.g. amqp://username:password@rabbitmq.mydomain.com:5672/%%2F",type=str,required=True)
        requiredNamed.add_argument('-q', '--queue',dest='queue',help='Name of RabbitMQ queue',type=str,required=True)
        requiredNamed.add_argument('-o', '--output','-i', '--input',dest='file',help='Output or Input file where messages are stored',required=True,type=str)
        args_dict = vars(parser.parse_args())

        if args_dict['receive']:
            args_dict['action'] = 'receive'
        elif args_dict['delete']:
            args_dict['action'] = 'delete'
        else:
            args_dict['action']='NA'
        return args_dict

def main(argv):
    #initialize logging
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    LOGGER = logging.getLogger('sasrabbitmq')

    #Read input options
    args_dict = getArguments()
    action = args_dict['action']
    amqp_uri = args_dict['uri']
    filename = args_dict['file']
    queue = args_dict['queue']

    if 'amqp://' in amqp_uri:
        try:
            LOGGER.info("Action: %s" % action)
            LOGGER.info("URI: %s" % amqp_uri)
            LOGGER.info("Queue: %s" % queue)
            LOGGER.info("Filename: %s" % filename)
            #consumer = Consumer(amqp_uri,action,queue,filename)
            #consumer.run()
        except KeyboardInterrupt:
            #consumer.stop()
            LOGGER.info('Stop')
    else:
        LOGGER.info("Invalid AMQP URI: %s" % amqp_uri)

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
