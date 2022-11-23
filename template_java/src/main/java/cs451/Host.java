package cs451;
import cs451.Primitives.Application;
import cs451.Primitives.FIFOChannel;

import java.io.*;
import java.net.*;
import java.util.*;

import static java.lang.Math.min;

public class Host {

    Application applicationLayer;
    List<Host> hostsList;

    public void setNumOfMsg(int numOfMsg) {
        this.numOfMsg = numOfMsg;
    }

    int numOfMsg;
    public void setApplicationLayer(Application applicationLayer) {
        this.applicationLayer = applicationLayer;
    }

    public Application getApplicationLayer() {
        return this.applicationLayer;
    }
    public void setHosts(List<Host> hosts) {
        this.hostsList = hosts;
    }

    class Logs {
        public String logString = "";
        public synchronized void addLog(String addition) {
            logString += addition;
            logString += "\n";
        }
    }
    final Logs logs = new Logs();
    private static final String IP_START_REGEX = "/";

    int lastMsg = 0;
    private int id;
    private String ip;
    private int port = -1;

    private String outputPath = "";

    public boolean populate(String idString, String ipString, String portString) {
        try {
            id = Integer.parseInt(idString);

            String ipTest = InetAddress.getByName(ipString).toString();
            if (ipTest.startsWith(IP_START_REGEX)) {
                ip = ipTest.substring(1);
            } else {
                ip = InetAddress.getByName(ipTest.split(IP_START_REGEX)[0]).getHostAddress();
            }

            port = Integer.parseInt(portString);
            if (port <= 0) {
                System.err.println("Port in the hosts file must be a positive number!");
                return false;
            }
        } catch (NumberFormatException e) {
            if (port == -1) {
                System.err.println("Id in the hosts file must be a number!");
            } else {
                System.err.println("Port in the hosts file must be a number!");
            }
            return false;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return true;
    }

    public int getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    private int current_batch = 0;

    private String getNextMsg(int intervalBegin, int numOfMsg) {
        String msg = "";
        for(int j = intervalBegin; j <= min(numOfMsg, intervalBegin + 8); j++) {
            applicationLayer.log("b", null, j);
            msg += Integer.toString(j);
            if(j != min(numOfMsg, intervalBegin + 8)) {
                msg += ",";
            }
        }
        this.intervalBegin += 9;
        return msg;
    }

    private int capacity = 1;

    private int batchIndex = 0;

    private FIFOChannel fifo_channel;
    private int intervalBegin;
    public void sendNextBatch() {
        if(intervalBegin > numOfMsg)
            return;
        String msg = "";
        applicationLayer.log("b", null, intervalBegin);
        msg += Integer.toString(intervalBegin);
        this.intervalBegin += 1;
        System.out.println("sending msg " + msg);
        this.fifo_channel.fifo_broadcast(msg);
    }
    public void start() {
        intervalBegin = 1;
        fifo_channel = new FIFOChannel(this.hostsList, this);

        this.sendNextBatch();
    }
}