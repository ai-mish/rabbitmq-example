options set=CLASSPATH "/tmp/rabbitmq/jars/jdk17/amqp-client-4.2.0.jar:/tmp/rabbitmq/jars/slf4j-log4j12.jar:/tmp/rabbitmq/jars/jdk17/rabbitmq-jms-1.7.0.jar:/tmp/rabbitmq/jars/providerutil-1.2.1.jar:/tmp/rabbitmq/jars/geronimo-jms_1.1_spec-1.1.1.jar:/tmp/rabbitmq/jars/slf4j-api.jar:/tmp/rabbitmq/jars/fscontext-4.6-b01.jar:/tmp/rabbitmq/jars/log4j.jar";


filename mymq JMS jndictxtfactory='com.sun.jndi.fscontext.RefFSContextFactory'
jndiproviderurl='file:///tmp/rabbitmq/jndi' connFactory='ConnectionFactory' timeout=500;

       data _null_;
           *infile mymq(sas.audit.queue);
           file mymq(sas.audit.queue);
           a=100; b=2000000000; c=3.14; d='Just a test string.';
           put a b c d;
      run;

filename mymq JMS jndictxtfactory='com.sun.jndi.fscontext.RefFSContextFactory'
jndiproviderurl='file:///tmp/rabbitmq/jndi' connFactory='ConnectionFactory' timeout=500;

       data _null_;
           infile mymq(sas.audit.queue);
           *file mymq(response);
           input;
           put _infile_;
      run;
