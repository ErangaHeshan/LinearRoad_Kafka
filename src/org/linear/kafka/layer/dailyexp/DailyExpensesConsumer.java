package org.linear.kafka.layer.dailyexp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.linear.kafka.input.HistoryLoadingNotifier;
import org.linear.kafka.events.ExpenditureEvent;
import org.linear.kafka.events.HistoryEvent;
import org.linear.kafka.util.Utilities;
import org.linear.kafka.events.PositionReportEvent;
import org.linear.kafka.util.Constants;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class DailyExpensesConsumer implements Runnable {
	private static Log log = LogFactory.getLog(DailyExpensesConsumer.class);
	private long currentSecond;
    private LinkedList<org.linear.kafka.events.ExpenditureEvent> expEvtList;
    private LinkedList<HistoryEvent> historyEvtList;
	private KafkaStream kStream;
	private int kThreadNumber;
	private final String out_going_topic = "output_topic"; 
	
	//We need a producer here to communicate the events to Output segment
	Producer<String, String> producerOutput;
	
	public DailyExpensesConsumer(KafkaStream aStream, int aThreadNumber){
    	this.currentSecond = -1;
		kStream = aStream;
		kThreadNumber = aThreadNumber;
        expEvtList = new LinkedList<ExpenditureEvent>();
        historyEvtList = new LinkedList<HistoryEvent>();
        
        //Each and every consumer object will have to load its own set of properties from the Properties file.
        Properties props = new Properties();       
        InputStream propertiesIS = DailyExpenses.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_DAILYEXP);

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
		
		String historyFile = props.getProperty(org.linear.kafka.util.Constants.LINEAR_HISTORY);
		
		HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
		notifierObj.start();//The notification server starts at this point
		
		loadHistoricalInfo(historyFile);
		
		notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
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
			
			posRptEvt.time = Long.parseLong(keyFields[1]);//keyFields[0] is the packet type
			posRptEvt.vid = Integer.parseInt(keyFields[2]);
			posRptEvt.speed = Byte.parseByte(valueFields[0]);
			posRptEvt.xWay = Byte.parseByte(valueFields[1]);
			posRptEvt.mile = Byte.parseByte(valueFields[4]);
			posRptEvt.offset = Short.parseShort(valueFields[5]);
			posRptEvt.lane = Byte.parseByte(valueFields[2]);
			posRptEvt.dir = Byte.parseByte(valueFields[3]);
			
			process(posRptEvt);*/
			
			ExpenditureEvent expEvt = new ExpenditureEvent();
			
//			public long time; //A timestamp measured in seconds since the start of the simulation
//			public int vid; //vehicle identifier
//			public int qid; //Query ID
//			public byte xWay; //Express way number 0 .. 9
//			public int day; //The day for which the daily expenditure value is needed
			
			//key = Constants.DAILY_EXP_EVENT_TYPE + " " + time + " " + vid;
			//value = xway + " " + qid + " " + day;	
			
			expEvt.time = Long.parseLong(fields[1]);
			expEvt.vid = Integer.parseInt(fields[2]);
			expEvt.xWay = Byte.parseByte(fields[3]);
			expEvt.qid = Byte.parseByte(fields[4]);
			expEvt.day = Byte.parseByte(fields[5]);
			
			process(expEvt);
		}
	}
	
    public void process(ExpenditureEvent evt){    	
		int len = 0;
		Statement stmt;
		
				Iterator<HistoryEvent> itr = historyEvtList.iterator();
				int sum = 0;
				while(itr.hasNext()){
					HistoryEvent histEvt = (HistoryEvent)itr.next();
					
					if((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.day)){
						sum += histEvt.daily_exp;
					}					
				}

				KeyedMessage<String, String> data = new KeyedMessage<String, String>(out_going_topic, (Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum), "");// Here we have to send at least an empty string as the value. Otherwise the send() will block foreaver.
				producerOutput.send(data);
				
				/*
				try{
				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

				    bytesMessage.writeBytes((Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum).getBytes());
				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				    producer_output.send(bytesMessage);
				}catch(JMSException e){
					e.printStackTrace();
				}*/
    }
    
	public void loadHistoricalInfo(String inputFileHistory) {
			BufferedReader in;
			try {
				in = new BufferedReader(new FileReader(inputFileHistory));
			
			String line;
			int counter = 0;
			int batchCounter = 0;
			int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK		
			Statement stmt;
			StringBuilder builder = new StringBuilder();
					
			log.info(Utilities.getTimeStamp() + " : Loading history data");
			while((line = in.readLine()) != null){			
				//#(1 8 0 55)
				/*
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				*/
				
				String[] fields = line.split(" ");
				fields[0] = fields[0].substring(2);
				fields[3] = fields[3].substring(0, fields[3].length() - 1);
				
				historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
			}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info(Utilities.getTimeStamp() + " : Done Loading history data");
			//Just notfy this to the input event injector so that it can start the data emission process
			try {
				PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
				writer.println("\n");
				writer.flush();
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			
	}
}
