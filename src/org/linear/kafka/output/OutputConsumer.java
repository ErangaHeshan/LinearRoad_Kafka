package org.linear.kafka.output;

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

public class OutputConsumer implements Runnable {
	private long currentSecond;
	
	private KafkaStream kStream;
	private int kThreadNumber;
		
	//We need a producer here to communicate the events to Toll segment
	Producer<String, String> producerSegStat;
	
	public OutputConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
		
        //Each and every consumer object will have to load its own set of properties from the Properties file.
        Properties props = new Properties();       
        InputStream propertiesIS = Output.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_OUTPUT);

        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_OUTPUT + "' not found in the classpath");
        }

        try {
			props.load(propertiesIS);
		} catch (IOException e) {
			e.printStackTrace();
		}
               
		kStream = aStream;
		kThreadNumber = aThreadNumber;
	}
	
	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> itr = kStream.iterator();
		
		while(itr.hasNext()){
			//Here we receive the message
			MessageAndMetadata<byte[], byte[]> kv = itr.next();
			
			System.out.println(new String(kv.key()) + " " + new String(kv.message()));		
		}
	}
}
