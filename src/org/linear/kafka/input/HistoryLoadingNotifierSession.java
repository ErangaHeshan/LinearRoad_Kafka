package org.linear.kafka.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class HistoryLoadingNotifierSession extends Thread{
	private Socket skt;
	private boolean statusFlag;
	private HistoryLoadingNotifier ref;
	
	public HistoryLoadingNotifierSession(HistoryLoadingNotifier refToMain, Socket s, boolean status){
		skt = s;
		statusFlag = status;
		ref = refToMain;
	}
	
	public void setStatus(boolean status){
		statusFlag = status;
	}
	
	public void run(){
		try {
			BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));
			PrintWriter out = new PrintWriter(skt.getOutputStream());
			String msg = null;
			
			//This object does not keep on reading what the other end tells it. It just reads the
			//content and sends some response (Thats why we do not have a while loop here). There is no need of maintaining a session.
			//but to be more accurate we have a session like mechanism which makes us to create
			//this class.
			if((msg = buff.readLine()) != null){
				if(msg.equals("done?")){
					if(statusFlag){
						out.println("yes");
						out.flush();
					}else{
						out.println("no");
						out.flush();
					}
				}else if(msg.equals("shtdn")){
					ref.shutdown();
				}else if(msg.equals("ruok")){
					out.println("imok");
					out.flush();
				}else{
					out.println("send");
					out.flush();
				}
			}
			
			buff.close();
			out.close();
			skt.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
