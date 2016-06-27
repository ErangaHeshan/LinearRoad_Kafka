package org.linear.kafka.layer.segstat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.linear.kafka.events.PositionReportEvent;
import org.linear.kafka.util.Constants;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SegmentStatisticsConsumer implements Runnable {
	private long currentSecond;
	private LinkedList<PositionReportEvent> posEvtList;
	private ArrayList<PositionReportEvent> evtListNOV;
	private ArrayList<PositionReportEvent> evtListLAV;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes
	
	private KafkaStream kStream;
	private int kThreadNumber;
	private final String out_going_topic = "toll_topic"; 
	
	//We need a producer here to communicate the events to Toll segment
	Producer<String, String> producerSegStat;
	
	public SegmentStatisticsConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
        posEvtList = new LinkedList<PositionReportEvent>();
        evtListNOV = new ArrayList<PositionReportEvent>();
        evtListLAV = new ArrayList<PositionReportEvent>();
		
        //Each and every consumer object will have to load its own set of properties from the Properties file.
        Properties props = new Properties();       
        InputStream propertiesIS = SegmentStatisticsConsumer.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        ProducerConfig config = new ProducerConfig(props);
        producerSegStat = new Producer<String, String>(config);
        
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
			
			
			String[] keyFields = (new String(kv.key())).split(" ");
			String[] valueFields = (new String(kv.message())).split(" ");
			
			//By default we know that the incomming events for Segstat will be of PositionReport event type.
			//therefore we do not need to check the packet type here.
			
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
			
			posRptEvt.time = Long.parseLong(keyFields[1]);//keyFields[0] is the packet type
			posRptEvt.vid = Integer.parseInt(keyFields[2]);
			posRptEvt.speed = Byte.parseByte(valueFields[0]);
			posRptEvt.xWay = Byte.parseByte(valueFields[1]);
			posRptEvt.mile = Byte.parseByte(valueFields[4]);
			posRptEvt.offset = Short.parseShort(valueFields[5]);
			posRptEvt.lane = Byte.parseByte(valueFields[2]);
			posRptEvt.dir = Byte.parseByte(valueFields[3]);
			
			process(posRptEvt);
		}
	}
	
    public void process(PositionReportEvent evt){       	
		if(currentSecond == -1){
			currentSecond = evt.time;
		}else{
			if((evt.time - currentSecond) > 60){
				System.out.println("AAAAAAA1");
				//synchronized(this){
					calculateNOV();
				//}
				System.out.println("AAAAAAA2");
				evtListNOV.clear();
				System.out.println("AAAAAAA3");
				currentSecond = evt.time;
				minuteCounter++;
				System.out.println("AAAAAAA4");
				
				if(minuteCounter >= lavWindow){
					System.out.println("AAAAAAA5");
					calculateLAV(currentSecond);
					System.out.println("AAAAAAA6");	
					
					//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
					//evtListLAV.clear();
					
					minuteCounter = 0;
				}
			}
		}
		
		evtListNOV.add(evt);
		evtListLAV.add(evt);
    }

	private void calculateLAV(long currentTime) {
		float result = -1;
		float avgVelocity = -1;
		
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  

		//First identify the number of segments
		Iterator<PositionReportEvent> itr = evtListLAV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		ArrayList<PositionReportEvent> tmpEvtListLAV = new ArrayList<PositionReportEvent>(); 
		float lav = -1;
		
		for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
			Iterator<Byte> segItr = segList.iterator();
			int vid = -1;
			byte mile = -1;
			ArrayList<Integer> tempList = null;
			PositionReportEvent evt = null;
			long totalSegmentVelocity = 0;
			long totalSegmentVehicles = 0;
			
			//We calculate LAV per segment
			while(segItr.hasNext()){
				mile = segItr.next();
				itr = evtListLAV.iterator();
				
				while(itr.hasNext()){
					evt = itr.next();
					
					if((Math.abs((evt.time - currentTime)) < 300)){
						if((evt.mile == mile) && (i == evt.dir)){ //Need only last 5 minutes data only
							vid = evt.vid;
							totalSegmentVelocity += evt.speed;
							totalSegmentVehicles++;
						}
						
						if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
							tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
						}
					}
				}
				
				lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
				if(!Float.isNaN(lav)){				
					/*try{
					    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

					    bytesMessage.writeBytes((Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i).getBytes());
					    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					    producer_toll.send(bytesMessage);
					}catch(JMSException e){
						e.printStackTrace();
					}*/

					//Message<String> dataPacket = new Message<String>();
					
					KeyedMessage<String, String> data = new KeyedMessage<String, String>(out_going_topic, (Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i), "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
					producerSegStat.send(data);
					
					totalSegmentVelocity = 0;
					totalSegmentVehicles = 0;
				}
			}
		}
			
		//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
		evtListLAV = tmpEvtListLAV;	
	}
	
	private void calculateNOV() {
		System.out.println("CCCCCCCC1");
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
		
		//Get the list of segments first
		Iterator<PositionReportEvent> itr = evtListNOV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		System.out.println("CCCCCCCC2");
		
		Iterator<Byte> segItr = segList.iterator();
		int vid = -1;
		byte mile = -1;
		ArrayList<Integer> tempList = null;
		PositionReportEvent evt = null;

		//For each segment		
		while(segItr.hasNext()){
			mile = segItr.next();
			itr = evtListNOV.iterator();
			while(itr.hasNext()){
				evt = itr.next();
				
				if(evt.mile == mile){
					vid = evt.vid;
					
					if(!htResult.containsKey(mile)){
						tempList = new ArrayList<Integer>();
						tempList.add(vid);
						htResult.put(mile, tempList);
					}else{
						tempList = htResult.get(mile);
						tempList.add(vid);
						
						htResult.put(mile, tempList);
					}
				}
			}
		}
		
		System.out.println("CCCCCCCC3");
		
		Set<Byte> keys = htResult.keySet();
		
		Iterator<Byte> itrKeys = keys.iterator();
		int numVehicles = -1;
		mile = -1;
		
		while(itrKeys.hasNext()){
			mile = itrKeys.next();
			numVehicles = htResult.get(mile).size();
			
			/*try{
			    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

			    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + ((int)Math.floor(currentSecond/60)) + " " + mile + " " + numVehicles).getBytes());
			    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
			    producer_toll.send(bytesMessage);
			}catch(JMSException e){
				e.printStackTrace();
			}*/
			System.out.println("CCCCCCCC3-1");
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(out_going_topic, (Constants.NOV_EVENT_TYPE + " " + ((int)Math.floor(currentSecond/60)) + " " + mile + " " + numVehicles), "");// Here we have to send at least an empty string as the value. Otherwise the send() will block forever.
			System.out.println("CCCCCCCC3-2");
			producerSegStat.send(data);
			System.out.println("CCCCCCCC3-3");
		}
		
		System.out.println("CCCCCCCC4");
	}
}
