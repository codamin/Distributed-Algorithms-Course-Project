package cs451;
import cs451.Primitives.Application;
import cs451.Primitives.Consensus;
import cs451.Primitives.Messages.Proposal;

import java.net.*;
import java.util.*;

import static java.lang.Math.min;

public class Host {
    Application applicationLayer;

    public List<Host> getHostsList() {
        return hostsList;
    }

    List<Host> hostsList;
    ArrayList<HashSet<Integer>> proposals;

    public void setProposals(ArrayList<HashSet<Integer>> proposals) {
        this.proposals = proposals;
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
    private int capacity = 10;
    private int  msgPerPacket = 8;

    private Consensus consensus;
    private Integer last_sent = 0;
//    public void sendNextBatch() {
//        consensus.propose(proposals.get(last_sent));
//        last_sent += 1;
//    }

    public void start() {
        int NUMPROC = this.hostsList.size() + 1;
        int NUMMSG = numOfMsg + 1;

        this.consensus = new Consensus(this, NUMPROC, NUMMSG, proposals);

        this.consensus.start();

//        for(HashSet<Integer> proposal: this.proposals) {
//            System.out.println("proposing:" + proposal);
//            consensus.propose(proposal);
//        }
    }
}