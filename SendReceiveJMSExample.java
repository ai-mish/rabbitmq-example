import javax.jms.*;
//import com.rabbitmq.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class SendReceiveJMSExample {

    private Context ctx;
    private Connection connection;
    private MessageConsumer messageConsumer;
    private MessageProducer messageProducer;
    private Session session;

    public SendReceiveJMSExample() {
    }

    public static void main(String[] args) throws Exception {
        SendReceiveJMSExample example = new SendReceiveJMSExample();
        example.runTest();
    }

    private void runTest() throws Exception {
      Properties env = new Properties();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
      env.put(Context.PROVIDER_URL, "file:///tmp/rabbitmq/jndi");
      Properties properties = new Properties();

      try {
        ctx = new InitialContext(env);
        ConnectionFactory connectionFactory
            = (ConnectionFactory) ctx.lookup("ConnectionFactory");
        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = (Queue) ctx.lookup("sas.audit.queue");

        messageConsumer = session.createConsumer(queue);
        MessageProducer messageProducer = session.createProducer(queue);

        TextMessage message = session.createTextMessage("Hello world!");
        messageProducer.send(message);
        session.commit();

        message = (TextMessage)messageConsumer.receive(1000);
        session.commit();
        System.out.println(message.getText());

        session.close();
        messageConsumer.close();
        messageProducer.close();
        connection.close();
        ctx.close();
  }
      catch (Exception e) {
        //System.out.println(e);
         e.printStackTrace();
         session.close();
         messageConsumer.close();
         messageProducer.close();
         connection.close();
         ctx.close();
      }
      finally {
        connection.close();
        ctx.close();
      }

      //properties.load(this.getClass().getResourceAsStream("/tmp/rabbitmq/jndi/helloworld.properties"));
      //Context context = new InitialContext(properties);





    }
}
