
package com.pinterest.secor.util;

import java.util.HashMap;
import java.util.ArrayList;
/**
 *
 * @author Ali Rakhshanfar
 */
public class PGStatsClient {
    private String host;
    private int port;
    private String source;

    private HashMap<String, double> queue;

    public PGStatsClient(String host, int port){
        this.host = host;
        this.port = port;
        this.queue = new HashMap<String, double>();
        this.source = "secor";
    }

    public void flush(){
        ArrayList<String> metrics = new ArrayList<String>();
        for (Map.Entry<String, double> entry : queue.entrySet()){
            metrics.add(String.format("%s:%s|c", entry.getKey(), entry.getValue()));
        }
        if(!metrics.isEmpty()){
            pushData(StringUtils.join(metrics, "\n"))
        }
    }
    private void pushData(String data){
        try{
            InetAddress IP = InetAddress.getByName(host);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData = new byte[1024];
            sendData = data.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IP, port);       
            clientSocket.send(sendPacket);       
            clientSocket.close();
        } catch(java.net.SocketException e){
            System.out.println("SocketException while sending stats.");
        } catch(java.io.IOException e2){
            System.out.println("IOException while sending stats.");
        }
    }
    public void pushCounter(String metric, double value){
        String key = String.format("%s.%s", this.source, metric);
        if(queue.containsKey(key)){
            value += queue.get(key);
        }
        queue.put(key, value);
    }
    public void pushCounter(String metric, int value){
        pushCounter(metric, (double) value);
    }
}
