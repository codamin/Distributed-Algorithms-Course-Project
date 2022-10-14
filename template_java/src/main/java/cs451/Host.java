package cs451;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Host {

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

    private HashMap<Integer, HashSet<Integer>> delivered = new HashMap<>();
    private HashMap<Integer, HashSet<Integer>> ackedSet = new HashMap<>();


    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    private Integer getHostId(String IP, int port) {
        return host2IdMap.get(IP + ":" + port);
    }

    private void pp2pSend(String destIP, int destPort, DatagramSocket socket, Integer msgSeqNumber) {
        byte[] buf = msgSeqNumber.toString().getBytes();
        DatagramPacket sendingPacket = null;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }

        boolean isNotAcked;
        do {
            System.out.println("Acked Set:");
            System.out.println(ackedSet);
            try {socket.send(sendingPacket);} catch (IOException e) {throw new RuntimeException(e);}

            isNotAcked = ackedSet.get(getHostId(destIP, destPort)) == null ||
                    ! ackedSet.get(getHostId(destIP, destPort)).contains(msgSeqNumber);
            System.out.println("isNotAcked");
            System.out.println(isNotAcked);
            System.out.println("msgSeqNumber");
            System.out.println(msgSeqNumber);

            try {Thread.sleep(200);} catch (InterruptedException e) {throw new RuntimeException(e);}

        } while(isNotAcked);

        // Write in output
        writeToOutput(new String("b"), 0, msgSeqNumber);
        System.out.println("Done.");
    }

    public void startSending(String destIP, int destPort, int numOfMsg) {
        // Creat a socket for sending the packet
        DatagramSocket socket;
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        new Thread(() -> {
            listenForAck(destIP, destPort, socket);
        }).start();

        for(Integer i = 1; i <= numOfMsg; i++) {
            System.out.println("Sending msg: " + i);
            pp2pSend(destIP, destPort, socket, i);
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
                System.out.println(host2IdMap);
                System.out.println("senderIp = " + senderIp);
                System.out.println("senderPort = " + senderPort);
                int senderId = getHostId(senderIp, senderPort);

                // handle the packet
                String msg = new String(packet.getData(), 0, packet.getLength());

                int msgSeqNumber = Integer.parseInt(msg);

                System.out.println("Delivered Set: ");
                System.out.println(delivered);
                // send ack
                boolean isNotDelivered = delivered.get(senderId) == null || ! delivered.get(senderId).contains(msgSeqNumber);
                if(delivered.get(senderId) == null)
                    System.out.println("delivered.get(senderId) = null");
                else if(! delivered.get(senderId).contains(msgSeqNumber))
                    System.out.println("delivered.get(senderId).contains(msgSeqNumber) = False");

                System.out.printf("Sending acknowledge for msg:" + msgSeqNumber);
                sendAck(senderIp, senderPort, msgSeqNumber, socket);
                System.out.println("Done.");

                if(isNotDelivered) {
                    // Deliver
                    pp2pDeliver(senderId, msgSeqNumber);
                }
            }).start();
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
            System.out.println(host2IdMap);
            System.out.println("senderIp = " + senderIp);
            System.out.println("senderPort = " + senderPort);
            int senderId = getHostId(senderIp, senderPort);

            // handle the packet
            String msg = new String(packet.getData(), 0, packet.getLength());

            System.out.println("Received: " + msg);

            // Add seq number to acked set
            String[] msgSplit = msg.split("#");
            int msgSeqNumber = 0;
            if (msgSplit[0].equals("ack")) {
                msgSeqNumber = Integer.parseInt(msgSplit[1]);
                if (ackedSet.get(senderId) == null) {
                    ackedSet.put(senderId, new HashSet<>());
                    ackedSet.get(senderId).add(msgSeqNumber);
                } else {
                    ackedSet.get(senderId).add(msgSeqNumber);
                }
            }
        }
    }

    void writeToOutput(String typeOfOperation, Integer senderId, Integer msgSeqNumber) {
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        if(typeOfOperation.equals("d"))
            writer.println(typeOfOperation + " " + senderId + " " + msgSeqNumber.toString());
        else if(typeOfOperation.equals("b"))
            writer.println(typeOfOperation + " " + msgSeqNumber.toString());
        writer.close();
    }
    public void pp2pDeliver(Integer senderId, Integer msgSeqNumber) {
        // Add to the delivered set
        System.out.println("Adding to the delivered set...");
        if(delivered.get(senderId) == null) {
            delivered.put(senderId, new HashSet<>());
            delivered.get(senderId).add(msgSeqNumber);
        }
        else {
            delivered.get(senderId).add(msgSeqNumber);
        }
        System.out.println("Done.");

        // Write in output
        System.out.println("Writing to the output file...");
        writeToOutput(new String("d"), senderId, msgSeqNumber);
        System.out.println("Done.");
    }
}