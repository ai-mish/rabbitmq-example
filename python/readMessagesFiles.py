#!/usr/bin/env python

#Standar libraries
import logging
import sys
import time
import json
import io
import os
import glob
import shutil

#Pika module to consume messages from AMQP or RabbitMQ
import pika


# Make it work for Python 2+3 and with Unicode

try:
    to_unicode = unicode
except NameError:
    to_unicode = str

LOG_FORMAT = ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
MESSAGE_FILE_PREFIX = "message"

class MessagesFiles(object):


    def __init__(self, path):
        """Create a new instance of the MessageFiles class, passing in the Messages File Path

        :param str filepath: Path where message files are saved in json format.

        """
        LOGGER = logging.getLogger('MessagesFiles.__init__')
        LOGGER.debug('Path set:')
        LOGGER.debug(path)
        self._filepath = path


    def getMultipleFileNames(self):
        LOGGER = logging.getLogger('MessagesFiles.getMultipleFileNames')
        files_path = []
        if self._filepath:
            try:
                if os.path.isdir(self._filepath) and os.path.exists(self._filepath):
                    LOGGER.debug('List file names to read')
                    LOGGER.info("Read from %s" % self._filepath + "/" + MESSAGE_FILE_PREFIX + "*")
                    files_path = glob.glob(self._filepath + "/" + MESSAGE_FILE_PREFIX + "*")
                    LOGGER.debug(files_path)
                else:
                    LOGGER.error('Directory does not exist')
                    LOGGER.error(self._filepath)
            except (IOError, OSError) as e:
                LOGGER.error(e)
            except Exception as err:
                LOGGER.error(err)
        else:
            LOGGER.error('File path is blank')

        return files_path

class Consumer(object):
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
        self._queue_state = None
        self._queue_size = 0
        self._messages_fetch_limit = 2
        self._message_consume_counter = 0
        self._messages_dict = []
        self._localMessageIds = []
        self._fileList = []
        self._destinationFolder = None

    def setSourceFileList(self,fileList):
        self._fileList = fileList

    def setDestinationFolder(self,destination):
        self._destinationFolder = destination

    def loadLocalMessageIds(self):
        LOGGER = logging.getLogger('Consumer.loadLocalMessageIds')
        try:
            for file in self._fileList:
                with open(file) as json_data:
                    messages = json.load(json_data)
                    for message in messages:
                        LOGGER.debug("Acknowledging Message %s" % message['id'])
                        self._localMessageIds.append(message['id'])
        except Exception as err:
            LOGGER.error(err)

    def isMessageExist(self,id):
        if id in self._localMessageIds:
            return True
        else:
            return False

    def moveFiles(self):
        LOGGER = logging.getLogger('Consumer.moveFiles')
        try:
            for file in self._fileList:
                if (MESSAGE_FILE_PREFIX in file):
                    LOGGER.debug('Moving file')
                    LOGGER.debug('Source: %s' % file)
                    LOGGER.debug('Destination: %s' % self._destinationFolder)
                    shutil.move(file, self._destinationFolder)
                else:
                    LOGGER.info('Not moving')
        except Exception as err:
            LOGGER.error(err)


    def start(self):
        LOGGER = logging.getLogger('Consumer.start')
        if self.isMessageExist('de12a40d-6629-48df-9a28-72b8ad1a6c54'):
            LOGGER.info("Acknowledging Message de12a40d-6629-48df-9a28-72b8ad1a6c54")
        else:
            LOGGER.info("Message de12a40d-6629-48df-9a28-72b8ad1a6c54 not found in local store")

        self.moveFiles()



def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    LOGGER = logging.getLogger(__file__)
    message = MessagesFiles(acceptedDir())
    try:
        consumer = Consumer('amqp://guest:guest@pdcesx16063.exnet.sas.com:5672/%2F')
        consumer.setSourceFileList(message.getMultipleFileNames())
        consumer.setDestinationFolder(archiveDir())
        consumer.loadLocalMessageIds()
        consumer.start()
    except KeyboardInterrupt:
        #consumer.stop()
        LOGGER.info('Stop')
    except Exception as err:
        LOGGER.error(err)

def getHomeDir():
    LOGGER = logging.getLogger('MessagesFiles.getHomeDir')
    APP_DIR = os.path.dirname(os.path.realpath(__file__))
    LOGGER.info('App Home: %s' % os.path.realpath(APP_DIR))
    return APP_DIR

def acceptedDir():
    REJECTED_DIR=os.path.join(getHomeDir(), 'accepted')
    return REJECTED_DIR

def rejectedDir():
    REJECTED_DIR=os.path.join(getHomeDir(), 'rejected')
    return REJECTED_DIR

def archiveDir():
    REJECTED_DIR=os.path.join(getHomeDir(), 'archive')
    return REJECTED_DIR

def logsDir():
    LOGS_DIR=os.path.join(getHomeDir(), 'logs')
    return LOGS_DIR

if __name__ == '__main__':
    main()
