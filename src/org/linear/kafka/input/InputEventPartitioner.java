/**
 * 
 */
package org.linear.kafka.input;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author miyuru
 *
 */
public class InputEventPartitioner implements Partitioner<String>{

	public InputEventPartitioner(VerifiableProperties props){
		
	}
	
	@Override
	public int partition(String key, int numPartitions) {
		int partition = 0;
//		int offset = key.lastIndexOf('.');
//		
//		if(offset > 0){
//			partition = Integer.parseInt(key.substring(offset + 1)) % numPartitions;
//		}
		
		return partition;
	}

}
