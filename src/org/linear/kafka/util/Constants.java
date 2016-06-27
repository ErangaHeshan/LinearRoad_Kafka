package org.linear.kafka.util;


public class Constants
{
    public static final String CONFIG_FILENAME_INPUT = "kafka-LinearRoad-Input.properties";
    public static final String CONFIG_FILENAME_SEGSTAT = "kafka-LinearRoad-Segstat.properties";
    public static final String CONFIG_FILENAME_TOLL = "kafka-LinearRoad-Toll.properties";
    public static final String CONFIG_FILENAME_ACCIDENT = "kafka-LinearRoad-Accident.properties";
    public static final String CONFIG_FILENAME_DAILYEXP = "kafka-LinearRoad-Dailyexp.properties";
    public static final String CONFIG_FILENAME_ACCBALANCE = "kafka-LinearRoad-Accbal.properties";
    public static final String CONFIG_FILENAME_OUTPUT = "kafka-LinearRoad-Output.properties";
    
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    
    public static final int POS_EVENT_TYPE = -1;
    public static final int ACC_BAL_EVENT_TYPE = -2;
    public static final int DAILY_EXP_EVENT_TYPE = -3;
    public static final int TRAVELTIME_EVENT_TYPE = -4;
    public static final int NOV_EVENT_TYPE = -5;
    public static final int LAV_EVENT_TYPE = -6;
    public static final int TOLL_EVENT_TYPE = -7;
    public static final int ACCIDENT_EVENT_TYPE = -8;
    
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    
    public static final String CLEAN_START = "clean-start";

	public static final String BROKER_LIST = "metadata.broker.list";
	public static final String SERIALIZER_CLASS = "serializer.class";
	public static final String PARTITIONER_CLASS = "partitioner.class";
	public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
	public static final String ZOOKEEPER = "zookeeper.connect";
	public static final String NUM_THREADS = "num.threads";//This many threads will be created in segment statistics listener
	public static final String HISTORY_COMPONENT_HOST = "history.host";//This is the host where the history component is run
}
