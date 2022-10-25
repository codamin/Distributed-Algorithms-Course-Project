package cs451;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import static java.lang.Math.min;

public class Host {

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

    private HashMap<String, Integer> host2IdMap;

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

    public void setHost2IdMap(HashMap<String, Integer> host2IdMap) {
        this.host2IdMap = host2IdMap;
    }

    HashMap<DatagramPacket, Integer> packetBeginMap = new HashMap<>();
    HashMap<DatagramPacket, Integer> packetEndMap = new HashMap<>();


    private volatile HashMap<Integer, HashSet<Integer>> deliveredSet = new HashMap<>();
    private volatile HashMap<Integer, HashSet<Integer>> ackedSet = new HashMap<>();

    private HashSet sentMsgs = new HashSet();

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    private Integer getHostId(String IP, int port) {
        return host2IdMap.get(IP + ":" + port);
    }

    private DatagramPacket prepareSendingPacket(String destIP, int destPort, int intervalBegin, int intervalEnd) {
        String msgString = String.valueOf(intervalBegin);
        for(int i = intervalBegin+1; i <= intervalEnd; i++) {
            msgString += "," + i;
        }
        byte[] buf = msgString.getBytes();
        DatagramPacket sendingPacket;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        packetBeginMap.put(sendingPacket, intervalBegin);
        packetEndMap.put(sendingPacket, intervalEnd);
        return sendingPacket;
    }
    private void sendPacket(DatagramPacket sendingPacket, DatagramSocket socket, int intervalBegin, int intervalEnd) {
        try {
            socket.send(sendingPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        boolean isNotSent = !sentMsgs.contains(intervalBegin);

        if (isNotSent) {
//            System.out.println("intervalBegin: " + intervalBegin);
//            System.out.println("intervalEnd: " + intervalEnd);

            for(int i = intervalBegin; i <= intervalEnd; i++)
            // Write in output
                log("b", 0, i);
        }

        sentMsgs.add(intervalBegin);
    }

    public void startSending(String destIP, int destPort, int numOfMsg) {
        // Creat a socket for sending the packet
        DatagramSocket socket;
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        new Thread(() -> {
            listenForAck(destIP, destPort, socket);
        }).start();

        ArrayList<DatagramPacket> sendingQueue = new ArrayList<>(1000);
        // create initial 1000 size sending set
            // update interval
        int channel_capacity = 1000;
        int packet_size = 8;
        int intervalBegin = 1;
        int intervalEnd = intervalBegin + packet_size - 1;

        for(int i = 0; i < channel_capacity; i++) {
            DatagramPacket sendingPacket = prepareSendingPacket(destIP, destPort, intervalBegin, intervalEnd);
            sendingQueue.add(sendingPacket);

            intervalBegin = intervalEnd + 1;
            intervalEnd = min(numOfMsg, intervalBegin + packet_size - 1);
            if(intervalBegin > numOfMsg)
                break;
        }

//        System.out.println("size of the queue is: " + sendingQueue.size());
//        System.out.println("interval begin is: " + intervalBegin);
//        System.out.println("interval end is: " + intervalEnd);
//        System.out.println("[N/8] is: " + (int) Math.ceil((double) numOfMsg / packet_size));

        boolean packetRemaining = true;
        while(packetRemaining) {
            packetRemaining = ackedSet.size() < (int) Math.ceil((double) numOfMsg / packet_size);
            Iterator<DatagramPacket> iterator = sendingQueue.iterator();
            while(iterator.hasNext()) {
                DatagramPacket currentPacket = iterator.next();

                boolean isNotAcked = ackedSet.get(getHostId(destIP, destPort)) == null ||
                        !ackedSet.get(getHostId(destIP, destPort)).contains(packetBeginMap.get(currentPacket));

                if (isNotAcked) {
                    System.out.println("sending packet: " + packetBeginMap.get(currentPacket));
                    sendPacket(currentPacket, socket, packetBeginMap.get(currentPacket), packetEndMap.get(currentPacket));
                }
                // else add another packet to the sending queue
                else {
                    //remove the current packet from the sending queue
                    System.out.println("removing packet: " + packetBeginMap.get(currentPacket));
                    iterator.remove();
                    // create another packet and add to array
                    if (intervalBegin <= numOfMsg) {
                        DatagramPacket sendingPacket = prepareSendingPacket(
                                destIP, destPort, packetBeginMap.get(currentPacket), packetEndMap.get(currentPacket));
                        sendingQueue.add(sendingPacket);
                        // update interval
                        intervalBegin = intervalEnd + 1;
                        intervalEnd = min(numOfMsg, intervalBegin + packet_size - 1);
                    }
                }
            }
        }
    }

    public void listenForAck(String destIP, int destPort, DatagramSocket socket) {
        while(true) {
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            // block to receive
            try {
                socket.receive(packet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String senderIp = packet.getAddress().getHostAddress();
            int senderPort = packet.getPort();
            int senderId = getHostId(senderIp, senderPort);

            // handle the packet
            String msg = new String(packet.getData(), 0, packet.getLength());

            // Add seq number to acked set
            String[] msgSplit = msg.split("#");
            int msgSeqNumber = 0;
            if (msgSplit[0].equals("ack")) {
                msgSeqNumber = Integer.parseInt(msgSplit[1]);

                boolean isNotAcked = ackedSet.get(getHostId(destIP, destPort)) == null ||
                        !ackedSet.get(getHostId(destIP, destPort)).contains(msgSeqNumber);

                if(isNotAcked) {
                    if (ackedSet.get(senderId) == null) {
                        ackedSet.put(senderId, new HashSet<>());
                        ackedSet.get(senderId).add(msgSeqNumber);
                    } else {
                        ackedSet.get(senderId).add(msgSeqNumber);
                    }
                }
            }
        }
    }

    private void sendAck(String destIP, int destPort, int msgSeqNumber, DatagramSocket socket) {
        byte[] buf = new String("ack" + "#" + msgSeqNumber).getBytes();
        DatagramPacket sendingPacket = null;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        try {socket.send(sendingPacket);} catch (IOException e) {throw new RuntimeException(e);}
    }

    public void startListening() {
        // create a null socket
        DatagramSocket socket;
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        while(true) {
            // create null packet
            byte[] buf = new byte[256];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);

            // block to receive
            try {socket.receive(packet);} catch (IOException e) {throw new RuntimeException(e);}

            new Thread(() -> {
                String senderIp = packet.getAddress().getHostAddress();
                int senderPort = packet.getPort();
                int senderId = getHostId(senderIp, senderPort);

                // handle the packet
                String msg = new String(packet.getData(), 0, packet.getLength());
                String[] msgSplit = msg.split(",");
                int msgSeqNumber = Integer.parseInt(msgSplit[0]);

                pp2pDeliver(senderId, Integer.parseInt(msgSplit[0]), Integer.parseInt(msgSplit[msgSplit.length-1]));

                sendAck(senderIp, senderPort, msgSeqNumber, socket);

            }).start();
        }
    }

    private void log(String typeOfOperation, Integer senderId, Integer msgSeqNumber) {
        if(typeOfOperation.equals("d")) {
            System.out.println("adding log to output 2");
            logs.addLog(typeOfOperation + " " + senderId + " " + msgSeqNumber.toString());
            System.out.println("logs = " + logs.logString);
        }
        else if(typeOfOperation.equals("b")) {
            logs.addLog(typeOfOperation + " " + msgSeqNumber.toString());
        }
    }

    public void writeLogs2Output() {
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        writer.println(logs.logString);
        writer.close();
    }
    public synchronized void pp2pDeliver(Integer senderId, Integer intervalBegin, Integer intervalEnd) {
        boolean isNotDelivered = deliveredSet.get(senderId) == null || ! deliveredSet.get(senderId).contains(intervalBegin);
        if(!isNotDelivered)
            return;
        // Add to the delivered set
        if(deliveredSet.get(senderId) == null) {
            deliveredSet.put(senderId, new HashSet<>());
            deliveredSet.get(senderId).add(intervalBegin);
        }
        else {
            deliveredSet.get(senderId).add(intervalBegin);
        }

        // Write in output
        for(int i = intervalBegin; i <= intervalEnd; i++) {
            log("d", senderId, i);
        }
    }
}