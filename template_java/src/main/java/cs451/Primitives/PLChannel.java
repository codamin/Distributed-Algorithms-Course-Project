package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;
import cs451.Message;
import cs451.PLMessage;

import java.io.IOException;
import java.net.*;
import java.util.*;

import static java.lang.Math.floorDiv;
import static java.lang.Math.min;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;
    private volatile HashSet<Message> deliveredSet = new HashSet<>();
    private volatile HashSet<Message> ackedSet = new HashSet<>();

    private Queue<PLMessage> deliverQueue = new LinkedList<>();
    private Queue<String> deliverQueue_msgContent = new LinkedList<>();
    private BEChannel upperChannel;

    public PLChannel(BEChannel beChannel, Host broadcaster, HashMap<String, HashMap<Integer, Integer>> host2IdMap) {
        this.broadcaster = broadcaster;
        this.host2IdMap = host2IdMap;
        try {
            this.socket = new DatagramSocket(this.broadcaster.getPort());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        this.upperChannel = beChannel;

        // start listening for acks and msgs
        new Thread(() -> listen()).start();
        new Thread(() -> deliverFromQueue()).start();
    }

    public void pl_send(String destIP, Integer destPort, Integer senderId, FIFOMessage fifoMsg) {
        System.out.println("ackedSet: " + ackedSet);
        String finalMsg = fifoMsg.getMsgContent() + "#" + fifoMsg.getOriginalSenderId() + "#" + fifoMsg.getSeqNumber();
        byte[] buf = finalMsg.getBytes();
        DatagramPacket msgPacket;
        try {
            msgPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }

        PLMessage msg = new PLMessage(host2IdMap.get(destIP).get(destPort), fifoMsg.getOriginalSenderId(), fifoMsg.getSeqNumber());

        int i = 0;
        while(! ackedSet.contains(msg)) {
            System.out.println("sending msg: " + msg);
            System.out.println(i + ": sending message:" +  finalMsg + " to: " + destPort);
            try {this.socket.send(msgPacket);} catch (IOException e) {throw new RuntimeException(e);}
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
//            System.out.println("pl-send");
            i += 1;
        }
        System.out.println("*******************************" + ackedSet);
        System.out.println("msg sent");
//            try {
//                Thread.sleep(300);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
    }


    private void pl_ack(String destIP, Integer destPort, Integer originalSenderId, Integer msgSeqNumber) {
        String ackMsg = "A" + "#" + originalSenderId + "#" + msgSeqNumber;
        System.out.println("sending ack: " + ackMsg);
        byte[] buf = ackMsg.getBytes();
        DatagramPacket sendingPacket;
        try {
            sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        try {
            socket.send(sendingPacket);
//            System.out.println("sent " + ackMsg + "to" + destIP + " " + destPort);
        } catch (IOException e) {throw new RuntimeException(e);}
    }

    private void deliverFromQueue() {
        while(true) {
            PLMessage msg = deliverQueue.peek();
            String msgContent = deliverQueue_msgContent.peek();
            if(msg != null) {
                pl_deliver(msg.getSenderId(), new FIFOMessage(msg.getSeqNumber(), msg.getOriginalSenderId(), msgContent));
                deliverQueue.poll();
                deliverQueue_msgContent.poll();
            }
        }
    }

    public void listen() {
        while(true) {
            byte[] rcvBuf = new byte[256];
            DatagramPacket rcvPacket = new DatagramPacket(rcvBuf, rcvBuf.length);

            // block to receive
            try {
                socket.receive(rcvPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String senderIp = rcvPacket.getAddress().getHostAddress();
            int senderPort = rcvPacket.getPort();
            String rcvdMsg = new String(rcvPacket.getData(), 0, rcvPacket.getLength());

//            System.out.println("rcvd from " + senderIp + " " + senderPort + " : " + msg);

            int senderId = host2IdMap.get(senderIp).get(senderPort);

            System.out.println("############################################ received msg: " + rcvdMsg);
            String[] msgSplit = rcvdMsg.split("#");

            // if the rcvd packet is an ack packet, add it to acked set
            Integer originalSenderId = Integer.parseInt(msgSplit[1]);
            Integer msgSeqNumber = Integer.parseInt(msgSplit[2]);

            // if it is an ack message
//            System.out.println(msgSplit[0]);

            if (msgSplit[0].equals("A")) {
                PLMessage msg = new PLMessage(senderId, originalSenderId, msgSeqNumber);

                if(! ackedSet.contains(msg))
                    ackedSet.add(msg);
            }
            // else if it is a deliver msg, deliver it to the upper channel, and ack it
            else {
                PLMessage msg = new PLMessage(senderId, originalSenderId, msgSeqNumber);
                pl_ack(senderIp, senderPort, originalSenderId, msgSeqNumber);
                if(!deliverQueue.contains(msg)) {
                    deliverQueue.add(msg);
                    deliverQueue_msgContent.add(msgSplit[0]);
                }
                System.out.println(deliverQueue.size());
            }
        }
    }
    private synchronized void pl_deliver(Integer senderId, FIFOMessage msg) {
        if(!deliveredSet.contains(msg)) {
            // Deliver the message
            upperChannel.be_deliver(senderId, new FIFOMessage(msg.getSeqNumber(), msg.getOriginalSenderId(), msg.getMsgContent()));
            deliveredSet.add(msg);
        }
    }
}
