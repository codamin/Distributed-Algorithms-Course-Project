package cs451;
import cs451.Primitives.Application;
import cs451.Primitives.Consensus;

import java.net.*;
import java.util.*;

import static java.lang.Math.min;

public class Host {
    Application applicationLayer;

    public List<Host> getHostsList() {
        return hostsList;
    }
    List<Host> hostsList;
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

    private Consensus consensus;

    private Scanner fd;

    public HashSet readNextProposal() {
        if(fd.hasNextLine()) {
            String[] parsedLine = fd.nextLine().split(" ");
            HashSet<Integer> proposal = new HashSet();
            for(String msg: parsedLine) {
                proposal.add(Integer.parseInt(msg));
            }
            return proposal;
        }

        return null;
    }

    public void start(Scanner fd, int numOfProposals, int max_elem_in_proposal, int max_distinct_elems) {
        this.fd = fd;
        int NUMPROC = this.hostsList.size();

        this.consensus = new Consensus(this, NUMPROC, max_distinct_elems);

        this.consensus.start();
    }
}