# rabbitmq-example

### network tcpdump -vvv -n host amqp.ip.addr -v -w amqp.cap

```
export CLASSPATH=.:/tmp/rabbitmq/jars/jdk17/rabbitmq-jms-1.7.0.jar:\
/tmp/rabbitmq/jars/jdk17/amqp-client-4.2.0.jar:\
/tmp/rabbitmq/jars/slf4j-log4j12.jar:\
/tmp/rabbitmq/jars/jdk17/rabbitmq-jms-1.7.0.jar:\
/tmp/rabbitmq/jars/providerutil-1.2.1.jar:\
/tmp/rabbitmq/jars/geronimo-jms_1.1_spec-1.1.1.jar:\
/tmp/rabbitmq/jars/slf4j-api.jar:\
/tmp/rabbitmq/jars/fscontext-4.6-b01.jar:\
/tmp/rabbitmq/jars/log4j.jar
```

```
java -Dlog4j.configuration=file:/opt/sas/sashome/SASFoundation/9.4/misc/tkjava/sas.log4j.properties SendReceiveJMSExample
```
