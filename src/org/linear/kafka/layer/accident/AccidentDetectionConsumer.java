package org.linear.kafka.layer.accident;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.linear.kafka.events.AccidentEvent;
import org.linear.kafka.layer.accident.Car;
import org.linear.kafka.events.PositionReportEvent;
import org.linear.kafka.util.Constants;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class AccidentDetectionConsumer implements Runnable {
	private long currentSecond;
	private KafkaStream kStream;
	private int kThreadNumber;
	private final String out_going_topic = "toll_topic"; 
	
	//We need a producer here to communicate the events to Toll segment
	Producer<String, String> producerToll;
	
	public AccidentDetectionConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
		kStream = aStream;
		kThreadNumber = aThreadNumber;
    	
    	//Each and every consumer object will have to load its own set of properties from the Properties file.
        Properties props = new Properties();       
        InputStream propertiesIS = AccidentDetection.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCIDENT);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCIDENT + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        ProducerConfig config = new ProducerConfig(props);
        producerToll = new Producer<String, String>(config);
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
			
			process(posRptEvt);*/
			
			onMessage(new String(kv.key()) + " " + new String(kv.message()));
		}
	}
	
    public void onMessage(String tuple)
    {
        String fields[] = tuple.split(" ");
        
        PositionReportEvent posRptEvt = new PositionReportEvent();
        
        /* 
				posRptEvt.time = Long.parseLong(fields[1]);//keyFields[0] is the packet type
				posRptEvt.vid = Integer.parseInt(fields[2]);
				posRptEvt.speed = Byte.parseByte(fields[3]);
				posRptEvt.xWay = Byte.parseByte(fields[4]);
				posRptEvt.mile = Byte.parseByte(fields[7]);
				posRptEvt.offset = Short.parseShort(fields[8]);
				posRptEvt.lane = Byte.parseByte(fields[5]);
				posRptEvt.dir = Byte.parseByte(fields[6]);
         */
        
        posRptEvt.time = Long.parseLong(fields[1]);//keyFields[0] is the packet type
		posRptEvt.vid = Integer.parseInt(fields[2]);
		posRptEvt.speed = Byte.parseByte(fields[3]);
		posRptEvt.xWay = Byte.parseByte(fields[4]);
		posRptEvt.mile = Byte.parseByte(fields[7]);
		posRptEvt.offset = Short.parseShort(fields[8]);
		posRptEvt.lane = Byte.parseByte(fields[5]);
		posRptEvt.dir = Byte.parseByte(fields[6]);       
        
		Car c = new Car(posRptEvt.time, posRptEvt.vid, posRptEvt.speed,
				posRptEvt.xWay, posRptEvt.lane, posRptEvt.dir, posRptEvt.mile);

		detect(c);        
    }

    public void detect(Car c) {	
		if (c.speed > 0) {
			remove_from_smashed_cars(c);
			remove_from_stopped_cars(c);
		} else if (c.speed == 0) {
			if (is_smashed_car(c) == false) {
				if (is_stopped_car(c) == true) {
					renew_stopped_car(c);
				} else {
					stopped_cars.add(c);
				}
				
				int flag = 0;
				for (int i = 0; i < stopped_cars.size() -1; i++) {
					Car t_car = (Car)stopped_cars.get(i);
					if ((t_car.carid != c.carid)&&(!t_car.notified)&&((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
							((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
							(c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
							(c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
							(c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {
						
						if (flag == 0) {
							AccidentEvent a_event = new AccidentEvent();
							a_event.vid1 = c.carid;
							a_event.vid2 = t_car.carid;
							a_event.xway = c.xway0;
							a_event.mile = c.mile0;
							a_event.dir = c.dir0;
							a_event.time = t_car.time;
							
							//out_going_topic
							
							KeyedMessage<String, String> data = new KeyedMessage<String, String>(out_going_topic, 
									(Constants.NOV_EVENT_TYPE + " " + a_event.vid1 + " " + a_event.vid2 + " " + a_event.xway + " " + a_event.mile + " " + a_event.dir), 
											"");// Here we have to send at least an empty string as the value. Otherwise the send() will block foreaver.
							producerToll.send(data);
							
							/*
							try{
							    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

							    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + a_event.vid1 + " " + a_event.vid2 + " " + a_event.xway + " " + a_event.mile + " " + a_event.dir ).getBytes());
							    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							    producer_toll.send(bytesMessage);
							}catch(JMSException e){
								e.printStackTrace();
							}*/
							
							
							
							t_car.notified = true;
							c.notified = true;
							flag = 1;
						}
						//The cars c and t_car have smashed with each other
						add_smashed_cars(c);
						add_smashed_cars(t_car);
						
						break;
					}
				}
			}
		}
	}
    
	public static LinkedList smashed_cars = new LinkedList();
	public static LinkedList stopped_cars = new LinkedList();
	public static LinkedList accidents = new LinkedList();
	
	public boolean is_smashed_car(Car car) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);

			if (((Car)smashed_cars.get(i)).carid == car.carid){
				return true;
			}
		}
		return false;
	}
	
	public void add_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove(i);
			}
		}
		smashed_cars.add(c);
	}
	
	public boolean is_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				return true;
			}
		}
		return false;
	}
	
	public void remove_from_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove();
			}
		}
	}
	
	public void remove_from_stopped_cars(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				stopped_cars.remove();
			}
		}
	}
	
	public void renew_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				c.xway3 = t_car.xway2;
				c.xway2 = t_car.xway1;
				c.xway1 = t_car.xway0;
				c.mile3 = t_car.mile2;
				c.mile2 = t_car.mile1;
				c.mile1 = t_car.mile0;				
				c.lane3 = t_car.lane2;
				c.lane2 = t_car.lane1;
				c.lane1 = t_car.lane0;
				c.offset3 = t_car.offset2;
				c.offset2 = t_car.offset1;
				c.offset1 = t_car.offset0;				
				c.dir3 = t_car.dir2;
				c.dir2 = t_car.dir1;
				c.dir1 = t_car.dir0;
				c.notified = t_car.notified;
				c.posReportID = (byte)(t_car.posReportID + 1);
				
				stopped_cars.remove(i);
				stopped_cars.add(c);
				
				//Since we already found the car from the list we break at here
				break;
			}
		}
	}
}
