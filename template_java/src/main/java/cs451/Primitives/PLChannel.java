package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;
import cs451.PLMessage;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;
    private HashSet<PLMessage> deliveredSet = new HashSet<>();
    private HashSet<PLMessage> ackedSet = new HashSet<>();
    private volatile Queue<PLMessage> deliverQueue = new LinkedList<>();
    private Queue<String> deliverQueue_msgContent = new LinkedList<>();

    private Integer CAPACITY = 10;
    private volatile LinkedBlockingQueue<DatagramPacket> resendingQueue= new LinkedBlockingQueue<>(CAPACITY);

    private volatile LinkedBlockingQueue<PLMessage> resendingQueueMsg = new LinkedBlockingQueue<>(CAPACITY);

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
        new Thread(() -> sendFromQueue(), "Send-Queue").start();
        new Thread(() -> listen(), "Listen").start();
        new Thread(() -> deliverFromQueue(), "Deliver-Queue").start();
    }

    private void sendFromQueue() {
        while(true) {
//            System.out.println("at the beginning g of send from queue");
            Iterator<DatagramPacket> packetIterator = resendingQueue.iterator();
            Iterator<PLMessage> plMessageIterator = resendingQueueMsg.iterator();

            int i = 0;
            while(i < CAPACITY && packetIterator.hasNext() && plMessageIterator.hasNext()) {
                i ++;
//                System.out.println("queue size: " + resendingQueue.size());
                DatagramPacket currentPacket = packetIterator.next();
                PLMessage currentPLMessage = plMessageIterator.next();
                System.out.println("current element:" + currentPLMessage);

                if(! ackedSet.contains(currentPLMessage)) {
//                    System.out.println("re-sending:" + currentPLMessage.getOriginalSenderId() + "#" + currentPLMessage.getSeqNumber());
                    try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
                }
                else if(ackedSet.contains(currentPLMessage)) {
                    System.out.println(resendingQueueMsg);
                    System.out.println("msg " + currentPLMessage.getOriginalSenderId() + "#" + currentPLMessage.getSeqNumber() + " is already acked by " + currentPLMessage.getSenderId());
                    packetIterator.remove();
                    plMessageIterator.remove();
                }
            }
//            DatagramPacket currentPacket = resendingQueue.peek();
//            PLMessage currentMessage = resendingQueueMsg.peek();
//
////            System.out.println(sendingQueue.size() + "," + sendingQueueMsg.size());
//
//            if((currentPacket != null) && (currentMessage != null) && (resendingQueue.size() == resendingQueueMsg.size())) {
//                resendingQueue.poll();
//                resendingQueueMsg.poll();
//                while(! ackedSet.contains(currentMessage)) {
//                    try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
//                }
//            }
        }
    }

    public void pl_send(String destIP, Integer destPort, Integer senderId, FIFOMessage fifoMsg) {
//        System.out.println("ackedSet: " + ackedSet);
        String finalMsg = fifoMsg.getMsgContent() + "#" + fifoMsg.getOriginalSenderId() + "#" + fifoMsg.getSeqNumber();
        byte[] buf = finalMsg.getBytes();
        DatagramPacket msgPacket;
        try {
            msgPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }


        // create a msg with sender=dest to see if it is acked or not
        PLMessage msg = new PLMessage(host2IdMap.get(destIP).get(destPort), fifoMsg.getOriginalSenderId(), fifoMsg.getSeqNumber());

        // send it once yourself
        try {this.socket.send(msgPacket);} catch (IOException e) {throw new RuntimeException(e);}

        // then add it to resend queue which will check and resend
        try {
            resendingQueue.put(msgPacket);
            resendingQueueMsg.put(msg);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void pl_ack(String destIP, Integer destPort, Integer originalSenderId, Integer msgSeqNumber) {
        String ackMsg = "A" + "#" + originalSenderId + "#" + msgSeqNumber;
//        System.out.println("sending ack: " + ackMsg);
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
//            System.out.println(Thread.currentThread().getName()+ ": waiting for deliver queue element");
            PLMessage msg = deliverQueue.peek();
            String msgContent = deliverQueue_msgContent.peek();
            if((msg != null) && (msgContent != null)) {
                pl_deliver(msg, msgContent);
                deliverQueue.poll();
                deliverQueue_msgContent.poll();
//                System.out.println(Thread.currentThread().getName()+ ": delivered msg: " + msg);
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

            int senderId = host2IdMap.get(senderIp).get(senderPort);

            String[] msgSplit = rcvdMsg.split("#");

            // if the rcvd packet is an ack packet, add it to acked set
            Integer originalSenderId = Integer.parseInt(msgSplit[1]);
            Integer msgSeqNumber = Integer.parseInt(msgSplit[2]);

            System.out.println("rcvd from " + senderId + " : " + rcvdMsg);

            // if it is an ack message
//            System.out.println(msgSplit[0]);

            if(msgSplit[0].equals("A")) {
                PLMessage msg = new PLMessage(senderId, originalSenderId, msgSeqNumber);
                if(! ackedSet.contains(msg)) {
//                    System.out.println("added " + msg + " to acked set");
                    ackedSet.add(msg);
//                    while(true);
                }
            }
            // else if it is a deliver msg, deliver it to the upper channel, and ack it
            else {
                PLMessage msg = new PLMessage(senderId, originalSenderId, msgSeqNumber);
                System.out.println("sending ACK to " + senderId + " :" + msg);
                pl_ack(senderIp, senderPort, originalSenderId, msgSeqNumber);
                if(! deliveredSet.contains(msg) && ! deliverQueue.contains(msg)) {
                    deliverQueue.add(msg);
                    deliverQueue_msgContent.add(msgSplit[0]);
//                    System.out.println("msg " + msg + " added to deliver queue");
                }
            }
        }
    }
    private void pl_deliver(PLMessage msg, String msgContent) {
//        System.out.println("delivered set: " + deliveredSet);
        if(!deliveredSet.contains(msg)) {
            // Deliver the message
            upperChannel.be_deliver(msg.getSenderId(), new FIFOMessage(msg.getSeqNumber(), msg.getOriginalSenderId(), msgContent));
            deliveredSet.add(msg);
        }
    }
}
