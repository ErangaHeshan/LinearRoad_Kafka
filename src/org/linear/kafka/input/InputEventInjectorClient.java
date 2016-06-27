package org.linear.kafka.input;

/**
 * This class first loads the historical data. Once the historical data is loaded we
 * start injecting tuples.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.kafka.util.Constants;
import org.linear.kafka.util.Utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.message.Message;

public class InputEventInjectorClient
{
	private static Log log = LogFactory.getLog(InputEventInjectorClient.class);
	private Producer<String, String> producerSegstat; //Although Segstat is a consumer, in this case we treat Segstat as a producer because thepackets need to go to the destination of Segstat.
	private Producer<String, String> producerAccDetect;
	private Producer<String, String> producerToll;
	private Producer<String, String> producerAccBalance;
	private Producer<String, String> producerDailyExp;
	
    private long tupleCounter = 0;
	private long uniqueTupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private long expStartTime = 0; 

    private long alpha = 0;
	private long currentTime = -1;
	private int dataRate = 0;
	private PrintWriter outLogger = new PrintWriter(new BufferedWriter(new FileWriter("input-injector-rate.csv", true)));
	
    public static void main(String[] args) throws Exception
    {   
    	System.out.println("AAAA");
        try
        {
            new InputEventInjectorClient();
        }
        catch (Throwable t)
        {
            //log.error(, t);
        	System.out.println("Error starting server shell client : " + t.getMessage());
            System.exit(-1);
        }	
        
        System.out.println("BBBB");
    }

    public InputEventInjectorClient() throws Exception
    {
   	    //First we get the input properties 
        Properties props = new Properties();       
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_INPUT);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_INPUT + "' not found in the classpath");
        }
        props.load(propertiesIS);
    	String carDataFile = props.getProperty(org.linear.kafka.util.Constants.LINEAR_CAR_DATA_POINTS);
    	
    	 //Next we create the producerSegstat object 
        props = new Properties();       
        propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_SEGSTAT + "' not found in the classpath");
        }
        props.load(propertiesIS);        
        ProducerConfig config = new ProducerConfig(props);
        producerSegstat = new Producer<String, String>(config);
        
        //Next we create the producerToll object 
        props = new Properties();       
        propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_TOLL);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_TOLL + "' not found in the classpath");
        }
        props.load(propertiesIS);        
        config = new ProducerConfig(props);
        producerToll = new Producer<String, String>(config);
        
        //Also we create producerAccDetectstat object
        props = new Properties();       
        propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCIDENT);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCIDENT + "' not found in the classpath");
        }
        props.load(propertiesIS);        
        config = new ProducerConfig(props);
        producerAccDetect = new Producer<String, String>(config);
        
        //Also we create producerAccBalance object
        props = new Properties();       
        propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCBALANCE);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_ACCBALANCE + "' not found in the classpath");
        }
        props.load(propertiesIS);        
        config = new ProducerConfig(props);
        producerAccBalance = new Producer<String, String>(config);
        
        //Also we create producerDailyExp object
        props = new Properties();       
        propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.kafka.util.Constants.CONFIG_FILENAME_DAILYEXP);
        if(propertiesIS == null){
        	throw new RuntimeException("Properties file '" + org.linear.kafka.util.Constants.CONFIG_FILENAME_DAILYEXP + "' not found in the classpath");
        }
        props.load(propertiesIS);        
        config = new ProducerConfig(props);
        producerDailyExp = new Producer<String, String>(config);
        
        //
        
        /*
        //Next we have to wait until the history information loading phase completes
        int c = 0;
        while(!HistoryLoadingNotifierClient.isHistoryLoaded()){
        	Thread.sleep(1000);//just wait one second and check again
        	System.out.println(c + " : isHistoryLoading...");
        	c++;
        }
        */
        System.out.println("Done loading the history....");
        //Next, we emit all the tuples from the injector.
        //May be this is not good software design because we are calling the tuple emission from the constructor
        
        emitTuples(carDataFile);
        System.out.println("Done emitting tuples. Now exitting...");
    }
    
    public void emitTuples(String carDataFile){
		try{
			BufferedReader in = new BufferedReader(new FileReader(carDataFile));
			
			String line;
			
			/*
			 
			 The input file has the following format
			 
			    Cardata points input tuple format
				0 - type of the packet (0=Position Report, 1=Account Balance, 2=Expenditure, 3=Travel Time [According to Richard's thesis. See Below...])
				1 - Seconds since start of simulation (i.e., time is measured in seconds)
				2 - Car ID (0..999,999)
				3 - An integer number of miles per hour (i.e., speed) (0..100)
				4 - Expressway number (0..9)
				5 - The lane number (There are 8 lanes altogether)(0=Ramp, 1=Left, 2=Middle, 3=Right)(4=Left, 5=Middle, 6=Right, 7=Ramp)
				6 - Direction (west = 0; East = 1)
				7 - Mile (This corresponds to the seg field in the original table) (0..99)
				8 - Distance from the last mile post (0..1759) (Arasu et al. Pos(0...527999) identifies the horizontal position of the vehicle as a meaure of number of feet
				    from the western most position on the expressway)
				9 - Query ID (0..999,999)
				10 - Starting milepost (m_init) (0..99)
				11 - Ending milepost (m_end) (0..99)
				12 - day of the week (dow) (0=Sunday, 1=Monday,...,6=Saturday) (in Arasu et al. 1...7. Probably this is wrong because 0 is available as DOW)
				13 - time of the day (tod) (0:00..23:59) (in Arasu et al. 0...1440)
				14 - day
				
				Notes
				* While Richard thesis explain the input tuple formats, it is probably not correct.
				* The correct type number would be 
				* 0=Position Report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
				* 2=Account Balance Queries (because all and only all the 4 fields available on tuples with type 2) (Type=2, Time, VID, QID)
				* 3=Daily expenditure (Type=3, Time, VID, XWay, QID, Day). Here if day=1 it is yesterday. d=69 is 10 weeks ago. 
				* 4=Travel Time requests (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
				
				history data input tuple format
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				
				E.g.
				#(1 3 0 31)
				#(1 4 0 61)
				#(1 5 0 34)
				#(1 6 0 30)
				#(1 7 0 63)
				#(1 8 0 55)
				
				//Note that since the historical data table's key is made out of several fields it was decided to use a relation table 
				//instead of using a hashtable
			 
			 */
			
			
			while((line = in.readLine()) != null){	
				System.out.println(line);
//				
//				System.out.println("Called : " + cnt);
//				cnt++;
				
				Long time = -1l;
				Integer vid = -1;
				Integer qid = -1;
				Byte spd = -1;
				Byte xway = -1;
				Byte mile = -1;
				Short ofst = -1;
				Byte lane = -1;
				Byte dir = -1;
				Integer day = -1;
				
				line = line.substring(2, line.length() - 1);
				
				String[] fields = line.split(" ");
				byte typeField = Byte.parseByte(fields[0]); 
				
				String key = null;
				String value = null;
				KeyedMessage<String, String> data = null;
				
				//In the case of Storm it seems that we need not to send the type of the tuple through the network. It is because 
				//the event itself has some associated type. This seems to be an advantage of Storm compared to other message passing based solutions.
				switch(typeField){
					case 0:
						//Need to calculate the offset value
						String offset = "" + ((short)(Integer.parseInt(fields[8]) - (Integer.parseInt(fields[7]) * 5280)));
						//This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						spd = Byte.parseByte(fields[3]);
						xway = Byte.parseByte(fields[4]);
						lane = Byte.parseByte(fields[5]);
						dir = Byte.parseByte(fields[6]);
						mile = Byte.parseByte(fields[7]);
						ofst = Short.parseShort(offset);
						
						//_collector.emit("position_report", new Values(time, vid, spd, xway, lane, dir, mile, ofst));
			            
						//Here we encode the type of event that we are emitting over Kafka topic.
						//Therefore a single topic may receive multiple different types of packets.
						//This enables us to process those in a single component. Otherwise if for each stream
						//we will get multiple Consumer objects spawned wich might not be much desirable (At least for the moment).
						key = Constants.POS_EVENT_TYPE + " " + time + " " + vid;
						value = spd + " " + xway + " " + lane + " " + dir + " " + mile + " " + ofst;
						
						data = new KeyedMessage<String, String>("segstat_topic", key, value);
						//+Miyuru: We must send a KeyedMessage here. The producer accepts only a List of KeyedMessages.
						//Therefore, we need to figureout what is the key to be sent over Kafka for all the data packets
						//we deal with.
						//Message msg = new Message(value.getBytes());
						//Furthermore, the Positionreport events are sought by Segstat, Accident detect, and toll modules
						producerSegstat.send(data);
						data = new KeyedMessage<String, String>("toll_topic", key, value);
						producerToll.send(data);
						data = new KeyedMessage<String, String>("accident_topic", key, value);
						producerAccDetect.send(data);
						break;
					case 2:
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						qid = Integer.parseInt(fields[9]);
						//This is an Account Balance report (Type=2, Time, VID, QID)
						//_collector.emit("accbal_report", new Values(time, vid, qid));
						
						key = Constants.ACC_BAL_EVENT_TYPE + " " + time + " " + vid;
						value = "" + qid; //Here we do not send the time and vid again. That is not required
						
						data = new KeyedMessage<String, String>("accbalance_topic", key, value);
						producerAccBalance.send(data);
						
						break;
					case 3 : 
						time = Long.parseLong(fields[1]);
						vid = Integer.parseInt(fields[2]);
						xway = Byte.parseByte(fields[4]);
						qid = Integer.parseInt(fields[9]);
						day = Integer.parseInt(fields[14]);
						//This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)
						//_collector.emit("daily_exp", new Values(time, vid, xway, qid, day));
						
						key = Constants.DAILY_EXP_EVENT_TYPE + " " + time + " " + vid;
						//value = time + " " + vid + " " + xway + " " + qid + " " + day;						
						value = xway + " " + qid + " " + day;
						
						data = new KeyedMessage<String, String>("dailyexp_topic", key, value);
						producerDailyExp.send(data);
						
						break;
					case 4:
						//This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
						break;
					case 5:
						System.out.println("Travel time query was issued : " + line);
						break;
				}
			}
			System.out.println("Done emitting the input tuples...");
		}catch(IOException ec){
			ec.printStackTrace();
		}
    }
}