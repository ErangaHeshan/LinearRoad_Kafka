package org.linear.kafka.layer.toll;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.kafka.events.AccidentEvent;
import org.linear.kafka.events.LAVEvent;
import org.linear.kafka.events.NOVEvent;
import org.linear.kafka.events.TollCalculationEvent;
import org.linear.kafka.layer.toll.AccNovLavTuple;
import org.linear.kafka.layer.toll.Car;
import org.linear.kafka.events.PositionReportEvent;
import org.linear.kafka.util.Constants;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TollConsumer implements Runnable {
	private long currentSecond;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes
	
	private KafkaStream kStream;
	private int kThreadNumber;
	
	//We need a producer here to communicate the events to Toll segment
	Producer<String, String> producerSegStat;
	
	private static Log log = LogFactory.getLog(TollConsumer.class);
    private int count;
    
	LinkedList cars_list = new LinkedList();
	HashMap<Integer, Car> carMap = new HashMap<Integer, Car>(); 
	HashMap<Byte, AccNovLavTuple> segments = new HashMap<Byte, AccNovLavTuple>();
	byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
	int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
	private LinkedList<PositionReportEvent> posEvtList = new LinkedList<PositionReportEvent>();

	private final String out_going_topic = "toll_topic"; 
	
	//We need a producer here to communicate the events to Toll segment
	Producer<String, String> output_accbalance_producer;
	Producer<String, String> output_output_producer;
	
	public TollConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
		kStream = aStream;
		kThreadNumber = aThreadNumber;
		
    	//Each and every consumer object will have to load its own set of properties from the Properties file.
		//Segstat
        Properties props = new Properties();       
        InputStream propertiesIS = Toll.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_TOLL);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_TOLL + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        ProducerConfig config = new ProducerConfig(props);
        producerSegStat = new Producer<String, String>(config);
        
        //Output for Output Component
        props = new Properties();       
        propertiesIS = Toll.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_OUTPUT);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_OUTPUT + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        config = new ProducerConfig(props);
        output_output_producer = new Producer<String, String>(config);
        
        //Output for AccountBalance Component
        props = new Properties();       
        propertiesIS = Toll.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCBALANCE);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCBALANCE + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        config = new ProducerConfig(props);
        output_accbalance_producer = new Producer<String, String>(config);
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
			
			//String[] fields = (new String(kv.key()) + " " + new String(kv.message())).split(" ");
			
			//String[] keyFields = (new String(kv.key())).split(" ");
			//String[] valueFields = (new String(kv.message())).split(" ");

			/*
			PositionReportEvent posRptEvt = new PositionReportEvent();
			
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
			
			posRptEvt.time = Long.parseLong(keyFields[0]);
			posRptEvt.vid = Integer.parseInt(keyFields[1]);
			posRptEvt.speed = Byte.parseByte(valueFields[0]);
			posRptEvt.xWay = Byte.parseByte(valueFields[1]);
			posRptEvt.mile = Byte.parseByte(valueFields[4]);
			posRptEvt.offset = Short.parseShort(valueFields[5]);
			posRptEvt.lane = Byte.parseByte(valueFields[2]);
			posRptEvt.dir = Byte.parseByte(valueFields[3]);
			
			process(posRptEvt);
			*/
			
			onMessage(new String(kv.key()) + " " + new String(kv.message()));
		}
	}
	
    public void onMessage(String tuple)
    {       
       String[] fields = tuple.split(" ");
       byte typeField = Byte.parseByte(fields[0]);
       
       switch(typeField){
       	   case Constants.POS_EVENT_TYPE:
				PositionReportEvent posRptEvt = new PositionReportEvent();
							
				posRptEvt.time = Long.parseLong(fields[1]);//keyFields[0] is the packet type
				posRptEvt.vid = Integer.parseInt(fields[2]);
				posRptEvt.speed = Byte.parseByte(fields[3]);
				posRptEvt.xWay = Byte.parseByte(fields[4]);
				posRptEvt.mile = Byte.parseByte(fields[7]);
				posRptEvt.offset = Short.parseShort(fields[8]);
				posRptEvt.lane = Byte.parseByte(fields[5]);
				posRptEvt.dir = Byte.parseByte(fields[6]);
       		   
       		   
       		   process(new PositionReportEvent(fields, true));
       		   break;
	       case Constants.LAV_EVENT_TYPE:
	    	   LAVEvent obj = new LAVEvent(Byte.parseByte(fields[1]), Float.parseFloat(fields[2]), Byte.parseByte(fields[3]));
	    	   lavEventOcurred(obj);
	    	   break;
	       case Constants.NOV_EVENT_TYPE:
	    	   try{
	    		   NOVEvent obj2 = new NOVEvent(Integer.parseInt(fields[1]), Byte.parseByte(fields[2]), Integer.parseInt(fields[3]));
	    		   novEventOccurred(obj2);
	    	   }catch(NumberFormatException e){
	    		   System.out.println("Not Number Format Exception for tuple : " + tuple);
	    	   }
	    	   break;
	       case Constants.ACCIDENT_EVENT_TYPE:
	    	   accidentEventOccurred(new AccidentEvent(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Byte.parseByte(fields[3]), Byte.parseByte(fields[4]), Byte.parseByte(fields[5]), Long.parseLong(fields[6])));
	    	   break;
       }
    }
    
	public void accidentEventOccurred(AccidentEvent accEvent) {
		System.out.println("Accident Occurred :" + accEvent.toString());
		boolean flg = false;
		
		synchronized(this){
			flg = segments.containsKey(accEvent.mile);
		}
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.isAcc = true;
			synchronized(this){
				segments.put(accEvent.mile, obj);
			}
		}else{
			synchronized(this){
				AccNovLavTuple obj = segments.get(accEvent.mile);
				obj.isAcc = true;
				segments.put(accEvent.mile, obj);
			}
		}
	}
    
    public void novEventOccurred(NOVEvent novEvent){
		boolean flg = false;

		flg = segments.containsKey(novEvent.segment);
	
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.nov = novEvent.nov;
			segments.put(novEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(novEvent.segment);
			obj.nov = novEvent.nov;

			segments.put(novEvent.segment, obj);
		}    	
    }
    
    public void lavEventOcurred(LAVEvent lavEvent){
		boolean flg = false;
		
		flg = segments.containsKey(lavEvent.segment); 
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(lavEvent.segment);
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}
    }

	public void process(PositionReportEvent evt){
		int len = 0;
		KeyedMessage<String, String> data = null;
        Iterator<Car> itr = cars_list.iterator();
	
		if(!carMap.containsKey(evt.vid)){
			Car c = new Car();
			c.carid = evt.vid;
			c.mile = evt.mile;
			carMap.put(evt.vid, c);
		}else{
			Car c = carMap.get(evt.vid);

				if(c.mile != evt.mile){ //Car is entering a new mile/new segment
					c.mile = evt.mile;
					carMap.put(evt.vid, c);

					if((evt.lane != 0)&&(evt.lane != 7)){ //This is to make sure that the car is not on an exit ramp
						AccNovLavTuple obj = null;
						
						obj = segments.get(evt.mile);

						if(obj != null){									
							if(isInAccidentZone(evt)){
								System.out.println("Its In AccidentZone");
							}
							
							if(((obj.nov < 50)||(obj.lav > 40))||isInAccidentZone(evt)){
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we set the toll to 0
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
																										
								String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
								
								data = new KeyedMessage<String, String>("accbalance_topic", msg, "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
								output_accbalance_producer.send(data);
								
								data = new KeyedMessage<String, String>("output_topic", msg, "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
								output_output_producer.send(data);
								
								/*
								try{
								    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
								    bytesMessage.writeBytes(msg.getBytes());
								    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								    producer_output.send(bytesMessage);
								}catch(JMSException e){
									e.printStackTrace();
								}
								
								try{
							          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
							          bytesMessage.writeBytes(msg.getBytes());
							          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							          producer_account_balance.send(bytesMessage);
									} catch (JMSException e) {
										e.printStackTrace();
									}
								*/
							}else{
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we need to calculate a toll
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
								
								if(segments.containsKey(evt.mile)){
									AccNovLavTuple tuple = null;
									
									synchronized(this){
										tuple = segments.get(evt.mile);
									}
																				
									tollEvt.toll = BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50);
																		
									String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
									
									
									data = new KeyedMessage<String, String>("accbalance_topic", msg, "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
									output_accbalance_producer.send(data);
									
									data = new KeyedMessage<String, String>("output_topic", msg, "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
									output_output_producer.send(data);
									
									/*
									try{
									    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

									    bytesMessage.writeBytes(msg.getBytes());
									    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
									    producer_output.send(bytesMessage);
									}catch(JMSException e){
										e.printStackTrace();
									}
									
									try{
								          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
								          bytesMessage.writeBytes(msg.getBytes());
								          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								          producer_account_balance.send(bytesMessage);
										} catch (JMSException e) {
											e.printStackTrace();
										}
									*/
								}
							}						
						}
					}
				}
		}
	}
    
	private boolean isInAccidentZone(PositionReportEvent evt) {
		byte mile = evt.mile;
		byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);
		
		while(mile < checkMile){
			if(segments.containsKey(mile)){
				AccNovLavTuple obj = segments.get(mile);
				
				if(Math.abs((evt.time - obj.time)) > 20){
					obj.isAcc = false;
					mile++;
					continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
				}
				
				if(obj.isAcc){
					return true;
				}
			}
			mile++;
		}
		
		return false;
	}
}
