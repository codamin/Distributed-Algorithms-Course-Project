package cs451;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

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

    private volatile HashMap<Integer, HashSet<Integer>> deliveredSet = new HashMap<>();
    private volatile HashMap<Integer, HashSet<Integer>> ackedSet = new HashMap<>();

    private HashSet sentMsgs = new HashSet();

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    private Integer getHostId(String IP, int port) {
        return host2IdMap.get(IP + ":" + port);
    }

    public static byte[] compress(byte[] in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream defl = new DeflaterOutputStream(out);
            defl.write(in);
            defl.flush();
            defl.close();

            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(150);
            return null;
        }
    }

    public static byte[] decompress(byte[] in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream infl = new InflaterOutputStream(out);
            infl.write(in);
            infl.flush();
            infl.close();

            return out.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(150);
            return null;
        }
    }

    private DatagramPacket prepareSendingPacket(String destIP, int destPort, int intervalBegin, int intervalEnd) {
        String msgString = String.valueOf(intervalBegin);
        for(int i = intervalBegin+1; i <= intervalEnd; i++) {
            msgString += "," + i;
        }
        byte[] buf = msgString.getBytes();
//        byte[] compressed_buf = compress(buf);
//        System.out.println("*****************************");
//        System.out.println("buf.length:" + buf.length);
//        System.out.println("compressed_buf.length:" + compressed_buf.length);
//        System.out.println("*****************************");
        DatagramPacket sendingPacket;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
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

        int channel_capacity = 4000;
        ArrayList<Integer> sendingQueue = new ArrayList<>();
        int packet_size = 8;
        int intervalBegin = 1;
        int intervalEnd = intervalBegin + packet_size - 1;

        for(int i = 0; i < channel_capacity; i++) {
            sendingQueue.add(intervalBegin);

            intervalBegin = intervalEnd + 1;
            intervalEnd = min(numOfMsg, intervalBegin + packet_size -1);
            if(intervalBegin >= numOfMsg)
                break;
        }

        boolean packetRemaining = true;

        ArrayList<Integer> newAdditions = new ArrayList<>();

        while(packetRemaining) {
            if(ackedSet.get(getHostId(destIP, destPort)) != null)
                if(ackedSet.get(getHostId(destIP, destPort)).size() == (int) Math.ceil((double) numOfMsg / packet_size))
                    packetRemaining = false;

            sendingQueue.addAll(newAdditions);

            newAdditions = new ArrayList<>();
            Iterator<Integer> iterator = sendingQueue.iterator();
            while(iterator.hasNext()) {
                int currentBeginInterval = iterator.next();
                int currentEndInterval = min(numOfMsg, currentBeginInterval + packet_size - 1);;
                DatagramPacket currentPacket = prepareSendingPacket(destIP, destPort, currentBeginInterval, currentEndInterval);

                boolean isNotAcked = ackedSet.get(getHostId(destIP, destPort)) == null ||
                        !ackedSet.get(getHostId(destIP, destPort)).contains(currentBeginInterval);

                if (isNotAcked) {
//                    System.out.println("sending packet: " + currentBeginInterval);
                    sendPacket(currentPacket, socket, currentBeginInterval, currentEndInterval);
                }
                // else add another packet to the sending queue
                else {
                    //remove the current packet from the sending queue
//                    System.out.println("removing packet: " + currentBeginInterval);
                    iterator.remove();
                    // create another packet and add to array
                    if (intervalBegin <= numOfMsg) {
                        newAdditions.add(intervalBegin);
                        // update interval
                        intervalBegin = intervalEnd + 1;
                        intervalEnd = min(numOfMsg, intervalBegin + packet_size -1);
                    }
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
//            String[] msgSplit = msg.split("#");
            int msgSeqNumber = 0;
//            if (msgSplit[0].equals("ack")) {
                msgSeqNumber = Integer.parseInt(msg);

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
//            }
        }
    }

    private void sendAck(String destIP, int destPort, int msgSeqNumber, DatagramSocket socket) {
//        byte[] buf = new String("ack" + "#" + msgSeqNumber).getBytes();
        byte[] buf = Integer.toString(msgSeqNumber).getBytes();

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

            String senderIp = packet.getAddress().getHostAddress();
            int senderPort = packet.getPort();
            int senderId = getHostId(senderIp, senderPort);

//            System.out.println(java.lang.Thread.activeCount());
            if(java.lang.Thread.activeCount() < 2) {
                new Thread(() -> {
                    // handle the packet
                    handleRcvdMsg(socket, packet, senderIp, senderPort, senderId);
                }).start();
            }
            else {
                // handle the packet
                handleRcvdMsg(socket, packet, senderIp, senderPort, senderId);
            }
        }
    }

    private void handleRcvdMsg(DatagramSocket socket, DatagramPacket packet, String senderIp, int senderPort, int senderId) {
//        byte[] msg_byte = decompress(packet.getData());
//        String msg = new String(msg_byte);
        String msg = new String(packet.getData(), 0, packet.getLength());
        String[] msgSplit = msg.split(",");
        int msgSeqNumber = Integer.parseInt(msgSplit[0]);

        System.out.println("msg:" + msg);
        pp2pDeliver(senderId, Integer.parseInt(msgSplit[0]), Integer.parseInt(msgSplit[msgSplit.length-1]));

        sendAck(senderIp, senderPort, msgSeqNumber, socket);
    }

    private void log(String typeOfOperation, Integer senderId, Integer msgSeqNumber) {
        if(typeOfOperation.equals("d")) {
//            System.out.println("adding log to output 2");
            logs.addLog(typeOfOperation + " " + senderId + " " + msgSeqNumber.toString());
//            System.out.println("logs = " + logs.logString);
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