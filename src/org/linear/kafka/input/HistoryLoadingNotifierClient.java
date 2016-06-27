/**
 * 
 */
package org.linear.kafka.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import org.linear.kafka.util.Constants;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifierClient {
	//private static Log log = LogFactory.getLog(DailyExpenses.class);
	
	public static boolean isHistoryLoaded(){
		boolean result = false;
		
		String host = Constants.HISTORY_COMPONENT_HOST;
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("done?");
			out.flush();
			
			String response = buff.readLine();
			if(response != null){
					if(response.trim().equals("yes")){
						result = true;
					}				
			}
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static boolean sendRUOK(){
		boolean result = false;
		String host = Constants.HISTORY_COMPONENT_HOST;
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("ruok");
			out.flush();
			
			String response = buff.readLine();
			if(response != null){
					if(response.trim().equals("imok")){
						result = true;
					}				
			}
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static void shutdownLoadingNotifier(){
		String host = Constants.HISTORY_COMPONENT_HOST;
		
		try {
			Socket skt = new Socket(host, Constants.HISTORY_LOADING_NOTIFIER_PORT);
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			
			out.println("shtdn");
			out.flush();
			out.close();
			buff.close();
			skt.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
