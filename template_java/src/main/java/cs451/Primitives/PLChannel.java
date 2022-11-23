package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.FullMessage;
import cs451.Host;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;

    private HashSet<FullMessage> deliveredSet = new HashSet<>();
    private HashSet<FullMessage> ackedSet = new HashSet<>();
    private volatile LinkedBlockingQueue<FullMessage> deliverQueue = new LinkedBlockingQueue<>();

    private volatile LinkedBlockingQueue<SendingQueueInfo> resendingQueue= new LinkedBlockingQueue<>();

    private BEChannel upperChannel;

    private Boolean busy = false;

    public PLChannel(BEChannel beChannel, Host broadcaster, HashMap<String, HashMap<Integer, Integer>> host2IdMap) {
        this.broadcaster = broadcaster;
        this.host2IdMap = host2IdMap;
        try {
            this.socket = new DatagramSocket(this.broadcaster.getPort());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        this.upperChannel = beChannel;
    }

    private void sendFromQueue() {
        while(true) {
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            SendingQueueInfo sendingQueueInfo = null;
            try {sendingQueueInfo = resendingQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}

            DatagramPacket currentPacket = createSendingPacket(sendingQueueInfo.destIP, sendingQueueInfo.destPort, sendingQueueInfo.fifoMsg);
            Integer destId = host2IdMap.get(sendingQueueInfo.destIP).get(sendingQueueInfo.destPort);

            if(! ackedSet.contains(new FullMessage(destId, sendingQueueInfo.fifoMsg))) {
//                System.out.println("not acked --> sending");
                try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
                try {resendingQueue.put(sendingQueueInfo);} catch (InterruptedException e) {throw new RuntimeException(e);}
            }
//            System.out.println("resendingQueue.size=" + resendingQueue.size());
//            Iterator<SendingQueueInfo> sendingQueueInfoIterator = resendingQueue.iterator();
//            while(sendingQueueInfoIterator.hasNext()) {
////                System.out.println("iterating...." + "size of sending queue is: " + resendingQueue.size());
//                SendingQueueInfo sendingQueueInfo = sendingQueueInfoIterator.next();
//                DatagramPacket currentPacket = createSendingPacket(sendingQueueInfo.destIP, sendingQueueInfo.destPort, sendingQueueInfo.fifoMsg);
//
//                Integer destId = host2IdMap.get(sendingQueueInfo.destIP).get(sendingQueueInfo.destPort);
//                if(! ackedSet.contains(new FullMessage(destId, sendingQueueInfo.fifoMsg))) {
//                    try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
////                    System.out.printf("sent msg" + sendingQueueInfo.fifoMsg);
//                }
//                else {
//                    sendingQueueInfoIterator.remove();
//                }
//            }
        }
    }

    public void startThreads() {
        // start listening for acks and msgs
        new Thread(() -> sendFromQueue(), "Send-Queue").start();
        new Thread(() -> listen(), "Listen").start();
        new Thread(() -> deliverFromQueue(), "Deliver-Queue").start();
    }

    private class SendingQueueInfo {

        public String destIP;
        public Integer destPort;
        public FIFOMessage fifoMsg;

        public SendingQueueInfo(String destIP, Integer destPort, FIFOMessage fifoMsg) {
            this.destIP = destIP;
            this.destPort = destPort;
            this.fifoMsg = fifoMsg;
        }
    }

    private DatagramPacket createSendingPacket(String destIP, Integer destPort, FIFOMessage fifoMsg) {
        String finalMsg = fifoMsg.getMsgContent() + "#" + fifoMsg.getOriginalSenderId() + "#" + fifoMsg.getSeqNumber();
        byte[] buf = finalMsg.getBytes();
        DatagramPacket msgPacket;
        try {
            msgPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        return msgPacket;
    }
    public void pl_send(String destIP, Integer destPort, Integer senderId, FIFOMessage fifoMsg) {

//        DatagramPacket msgPacket = creatSendingPacket(destIP, destPort, fifoMsg);
        // send it once yourself
//        try {this.socket.send(msgPacket);} catch (IOException e) {throw new RuntimeException(e);}

        SendingQueueInfo sendingQueueInfo = new SendingQueueInfo(destIP, destPort, fifoMsg);
        // then add it to resend queue which will check and resend
        try {
            resendingQueue.put(sendingQueueInfo);
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
//            System.out.println("starting delivery...");
//            System.out.println(System.currentTimeMillis() + " deliver queue size: " + deliverQueue.size());
            FullMessage fullMessage;
            // block to find msg
            try {fullMessage = deliverQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}
            deliveredSet.add(fullMessage);
            upperChannel.be_deliver(fullMessage.senderId, fullMessage.fifoMessage);
        }
    }

    public void listen() {
        while(true) {
            byte[] rcvBuf = new byte[128];
//            System.out.println(rcvBuf.length);
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

//            System.out.println("rcvd msg: " + rcvdMsg + " from: " + senderId);

            String[] msgSplit = rcvdMsg.split("#");

            // if the rcvd packet is an ack packet, add it to acked set
            Integer originalSenderId = Integer.parseInt(msgSplit[1]);
            Integer msgSeqNumber = Integer.parseInt(msgSplit[2]);

//            System.out.println("rcvd from " + senderId + " : " + rcvdMsg);
//            System.out.println(System.currentTimeMillis());

            // if it is an ack message
            if(msgSplit[0].equals("A")) {
                FullMessage msg = new FullMessage(senderId, new FIFOMessage(msgSeqNumber, originalSenderId, null));
                if(! ackedSet.contains(msg)) {
                    ackedSet.add(msg);
                }
            }
            // else if it is a deliver msg, deliver it to the upper channel, and ack it
            else {
                FullMessage msg = new FullMessage(senderId, new FIFOMessage(msgSeqNumber, originalSenderId, msgSplit[0]));
//                System.out.println("sending ACK to " + senderId + " :" + msg);
                pl_ack(senderIp, senderPort, originalSenderId, msgSeqNumber);
                if(! deliveredSet.contains(msg) && ! deliverQueue.contains(msg)) {
                    try {
                        deliverQueue.put(msg);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}