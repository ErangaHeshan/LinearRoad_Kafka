package org.linear.kafka.layer.accbalance;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.linear.kafka.events.AccountBalanceEvent;
import org.linear.kafka.events.PositionReportEvent;
import org.linear.kafka.util.Constants;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class AccountBalanceConsumer implements Runnable {
	private long currentSecond;
	
	private KafkaStream kStream;
	private int kThreadNumber;
	private final String out_going_topic = "output_topic";
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;
	
	//We need a producer here to communicate the events to Output segment
	Producer<String, String> producerOutput;
	
	public AccountBalanceConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
        this.accEvtList = new LinkedList<AccountBalanceEvent>();
        this.tollList = new HashMap<Integer, Integer>();
        
        //Each and every consumer object will have to load its own set of properties from the Properties file.
        Properties props = new Properties();       
        InputStream propertiesIS = AccountBalance.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        ProducerConfig config = new ProducerConfig(props);
        producerOutput = new Producer<String, String>(config);
        
		kStream = aStream;
		kThreadNumber = aThreadNumber;
	}
	
	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> itr = kStream.iterator();
		
		while(itr.hasNext()){
			//Here we receive the message
			//itr.next().message();
			
			//For the moment we just print the message
			//System.out.println("Thread : " + kThreadNumber + " : " + new String(itr.next().message()));
			MessageAndMetadata<byte[], byte[]> kv = itr.next();
			
			System.out.println(new String(kv.key()) + " " + new String(kv.message()));
			//System.out.println(new String(kv.message()));
			
			String[] fields = (new String(kv.key()) + " " + new String(kv.message())).split(" ");
			
			
			//String[] keyFields = (new String(kv.key())).split(" ");
			//String[] valueFields = (new String(kv.message())).split(" ");
			
			//By default we know that the incomming events for Segstat will be of PositionReport event type.
			//therefore we do not need to check the packet type here.
			
			//PositionReportEvent posRptEvt = new PositionReportEvent();
			
//			public long time; //A timestamp measured in seconds since the start of the simulation
//			public int vid; //vehicle identifier
//			public byte speed; // An integer number of miles per hour between 0 and 100 
//			public byte xWay; //Express way number 0 .. 9
//			public byte mile; //Mile number 0..99
//			public short offset; // Yards since last Mile Marker 0..1759
//			public byte lane; //Travel Lane 0..7. The lanes 0 and 7 are entrance/exit ramps
//			public byte dir; //Direction 0(West) or 1 (East)			
//			
			
			//						key = time + " " + vid;
			//value = spd + " " + xway + " " + lane + " " + dir + " " + mile + " " + ofst;
			
			/*
			posRptEvt.time = Long.parseLong(keyFields[1]);//keyFields[0] is the packet type
			posRptEvt.vid = Integer.parseInt(keyFields[2]);
			posRptEvt.speed = Byte.parseByte(valueFields[0]);
			posRptEvt.xWay = Byte.parseByte(valueFields[1]);
			posRptEvt.mile = Byte.parseByte(valueFields[4]);
			posRptEvt.offset = Short.parseShort(valueFields[5]);
			posRptEvt.lane = Byte.parseByte(valueFields[2]);
			posRptEvt.dir = Byte.parseByte(valueFields[3]);
			
			process(posRptEvt);
			*/
			
		    byte typeField = Byte.parseByte(fields[0]);
		       
	       switch(typeField){
	       	   case Constants.ACC_BAL_EVENT_TYPE:
	       		   
	       		   //						key = Constants.ACC_BAL_EVENT_TYPE + " " + time + " " + vid;
//					value = "" + qid; //Here we do not send the time and vid again. That is not required
//	       		public AccountBalanceEvent(long ttime, int tvid, int tqid) {	
//	       			this.time = ttime;//Seconds since start of simulation
//	       			this.vid = tvid;//Car ID
//	       			this.qid = tqid;//Query ID
//	       		}      		   
	       		   AccountBalanceEvent et = new AccountBalanceEvent(Long.parseLong(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
	       		   process(et);
	       		   System.out.println("ACC BAL Evt : " + et.toString());
	       		   break;
		       case Constants.TOLL_EVENT_TYPE:
		    	   //"" + vid + " " + toll + " " + segment;
		    	   //In the below lines fields[0] corresponds to the type of the event
		    	   int key = Integer.parseInt(fields[1]);//key is the vid
		    	   int value = Integer.parseInt(fields[2]); //value is the toll
		    	   
		    	   Integer kkey = (Integer)tollList.get(key);
		    	   
		    	   System.out.println("key : " + key + " value : " + value);
		    	   
		    	   if(kkey != null){
		    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the toll to the existing toll.
		    	   }else{
		    		   tollList.put(key, value);
		    	   }
		    	   
		    	   break;
	       }
			
		}
	}
	
    public void process(AccountBalanceEvent evt){
		int len = 0;
		Statement stmt;
		/*BytesMessage bytesMessage = null;
			    		
		if(evt != null){
				try{
				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
				    bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				    producer_output.send(bytesMessage);
				}catch(JMSException e){
					e.printStackTrace();
				}
		}*/
		
		//producerOutput
		//bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(out_going_topic, (Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)), ""); // Here we have to send at least an empty string as the value. Otherwise the send() will block foreaver. 
		producerOutput.send(data);
    }
}
