package home.vitaly.simulator;

import home.vitaly.datamodel.Transaction;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class EnqueueMessage
{

    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = "tcp://localhost:61616";
    private boolean transacted = false;
    private String subject = "TrQueue";
    private boolean persistent = true;
    private MessageProducer producer;
    private Session session;
    private Connection connection;
    
    public EnqueueMessage()  {
    }

    public void open() throws JMSException
    {
          
            Destination destination;
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);
     		connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(subject);
            producer = session.createProducer(destination);
            if (persistent) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            }

    }
    
  public void close() throws JMSException {
      producer.close();
      session.close();
      connection.close();
	  }
 
  
public void enqueueTransaction(Transaction tr) throws JMSException { 
	ObjectMessage message = session.createObjectMessage();
	message.setObject(tr);
		producer.send(message);
  	} 
}

