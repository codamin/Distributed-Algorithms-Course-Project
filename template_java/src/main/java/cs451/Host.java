package cs451;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

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

    private HashMap<Integer, ArrayList<Integer>> delivered = new HashMap<>();

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public void startSending(String destIP, int destPort, int numOfMsg) {

        // Creat a socket for sending the packet
        DatagramSocket socket = null;
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        for(Integer i = 1; i <= numOfMsg; i++) {
            byte[] buf = i.toString().getBytes();
            DatagramPacket sendingPacket = null;
            try {
                sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
            } catch (UnknownHostException e) {
                System.out.println("The given host name is unknown.");
                throw new RuntimeException(e);
            }

            try {
                socket.send(sendingPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Write in output
            System.out.println("Writing to the output file:");
            writeToOutput(new String("b"), i);
            System.out.println("Done.");
        }
    }

    public void startListening() {
        // create a null socket
        DatagramSocket socket = null;
        // create a null packet
        try {socket = new DatagramSocket(port);} catch (SocketException e) {throw new RuntimeException(e);}

        while(true) {
            byte[] buf = new byte[256];;
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            // block to receive
            try {socket.receive(packet);} catch (IOException e) {throw new RuntimeException(e);}

            String senderIp = packet.getAddress().getHostAddress() ;;
            int senderPort = packet.getPort();
            System.out.println(host2IdMap);
            System.out.println("senderIp = " + senderIp);
            System.out.println("senderPort = " + senderPort);
            int senderId = host2IdMap.get(senderIp + ":" + senderPort);

            // handle the packet
            String msg = new String(packet.getData(), 0, packet.getLength());
//        System.out.println("the received msg = " + msg);
            // Deliver
            int msgSeqNumber = Integer.parseInt(msg);
            pp2pDeliver(senderId, msgSeqNumber);
        }
    }


    public void listenForAck() {

    }

    void writeToOutput(String typeOfOperation, Integer msgSeqNumber) {
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        writer.println(typeOfOperation + " " + msgSeqNumber.toString());
        writer.close();
    }
    public void pp2pDeliver(Integer senderId, Integer msgSeqNumber) {
        // Write in output
        System.out.println("Writing to the output file:");
        writeToOutput(new String("d"), msgSeqNumber);
        System.out.println("Done.");

        // Add to the delivered set
        System.out.println("Adding to the Delivery Set");
        if(delivered.get(senderId) == null) {
            delivered.put(senderId, new ArrayList<>(msgSeqNumber));
        }
        else {
            delivered.get(senderId).add(msgSeqNumber);
        }
        System.out.println("Done.");
    }
}