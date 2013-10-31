package home.vitaly.simulator;


import home.vitaly.datamodel.TransactionImpl;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.document.mongodb.MongoOperations;
import org.springframework.data.document.mongodb.query.Criteria;
import org.springframework.data.document.mongodb.query.Query;

import javax.jms.JMSException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class RunSimulator {

    int fromHour = 3;      // пропустит от начала

	EnqueueMessage mq;
    MongoOperations mongoOperation;


	public static void main(String[] args) throws JMSException, UnknownHostException {


        if(args.length<1) System.out.println("First argument must be S -max or T -real time event speed");
		RunSimulator m = new RunSimulator();
		if(args.length==0 || args[0].equals("S")) { m.letsGoMaxSpeed(); }
		else  { m.letsGoRealTime(); }
	}		

 public RunSimulator() throws UnknownHostException {
     mq = new EnqueueMessage();
     ApplicationContext ctx = new ClassPathXmlApplicationContext("app-context.xml");
     mongoOperation = (MongoOperations) ctx.getBean("mongoTemplate");
 }

public void letsGoMaxSpeed() throws JMSException {

    System.out.println("Max speed events collection ...(read all transaction in memory)");
    List<TransactionImpl> listTr = mongoOperation.find(new Query(),TransactionImpl.class);
    System.out.println("Counter TR ready:");
    mq.open();
    int tik=0;
    for(TransactionImpl tr : listTr ) {
        mq.enqueueTransaction(tr);
        System.out.print(++tik + "\r");
        }
	}


public void letsGoRealTime() throws JMSException {
	mq.open();
	for(long tik=fromHour*60*60; tik < 24*60*60; tik++)
	{
		String timeStamp = getTimeStamp(tik);
        List<TransactionImpl> listTr = mongoOperation.find( new Query(Criteria.where("ttime").is(timeStamp)),TransactionImpl.class);

        System.out.print("\r"+timeStamp+" ");
		for(TransactionImpl tr : listTr ) {
			mq.enqueueTransaction(tr);
			System.out.print("*");
//            System.out.println("TR:"+tr);
        }
//		try 
//	    {
//	        Thread.sleep(1);
//	    } catch (InterruptedException e) 
//	    	{
//	    	mq.close();
//	    	System.exit(-1); 
//	    	}
		System.out.print("\n");
	}

}

private String getTimeStamp(Long seconds) {
	Date date = new Date(seconds * 1000);
	SimpleDateFormat sdf = new SimpleDateFormat("HHmmss", Locale.ENGLISH);
	sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
//	String formattedDate = sdf.format(date);
//	System.out.println(formattedDate); 
	return sdf.format(date);
	}
}

