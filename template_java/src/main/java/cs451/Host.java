package cs451;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;

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

    private void pp2pSend(String destIP, int destPort, DatagramSocket socket, Integer msgSeqNumber) {
        byte[] buf = msgSeqNumber.toString().getBytes();
        DatagramPacket sendingPacket;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        try {socket.send(sendingPacket);} catch (IOException e) {throw new RuntimeException(e);}

        boolean isNotSent = !sentMsgs.contains(msgSeqNumber);
        if(isNotSent) {
            // Write in output
            log("b", 0, msgSeqNumber);
        }

        sentMsgs.add(msgSeqNumber);
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println("Done.");
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
    public void startSending(String destIP, int destPort, int numOfMsg) {
        // Creat a socket for sending the packet
        DatagramSocket socket;
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        new Thread(() -> {
            listenForAck(destIP, destPort, socket);
        }).start();

        boolean flag = true;
        while(flag) {
            for (Integer i = 1; i <= numOfMsg; i++) {
                flag = false;
//                System.out.println("Sending msg: " + i);
                boolean isNotAcked = ackedSet.get(getHostId(destIP, destPort)) == null ||
                        !ackedSet.get(getHostId(destIP, destPort)).contains(i);
                if(isNotAcked) {
                    flag = true;
                    pp2pSend(destIP, destPort, socket, i);
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

//            boolean = lock

            new Thread(() -> {
                String senderIp = packet.getAddress().getHostAddress();
                int senderPort = packet.getPort();
                int senderId = getHostId(senderIp, senderPort);

                // handle the packet
                String msg = new String(packet.getData(), 0, packet.getLength());

                int msgSeqNumber = Integer.parseInt(msg);

                // send ack
//                boolean isNotDelivered = deliveredSet.get(senderId) == null || ! deliveredSet.get(senderId).contains(msgSeqNumber);
//
//                if(isNotDelivered) {
                    // Deliver
                pp2pDeliver(senderId, msgSeqNumber);
//                }

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
    public synchronized void pp2pDeliver(Integer senderId, Integer msgSeqNumber) {
        boolean isNotDelivered = deliveredSet.get(senderId) == null || ! deliveredSet.get(senderId).contains(msgSeqNumber);
        if(!isNotDelivered)
            return;
        // Add to the delivered set
        if(deliveredSet.get(senderId) == null) {
            deliveredSet.put(senderId, new HashSet<>());
            deliveredSet.get(senderId).add(msgSeqNumber);
        }
        else {
            deliveredSet.get(senderId).add(msgSeqNumber);
        }

        // Write in output
        log("d", senderId, msgSeqNumber);
    }
}