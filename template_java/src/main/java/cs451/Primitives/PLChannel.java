package cs451.Primitives;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static java.lang.Math.floorDiv;
import static java.lang.Math.min;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;

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
    }

    private volatile HashSet<LightMessage> deliveredSet = new HashSet<>();
    private volatile HashSet<LightMessage> ackedSet = new HashSet<>();

//    private Integer getHostId(String IP, Integer port) {
//        return host2IdMap.get(IP + ":" + port);
//    }
//    private DatagramPacket prepareSendingPacket(String msg) {
//        byte[] buf = msg.getBytes();
//        DatagramPacket sendingPacket = new DatagramPacket(buf, buf.length);
//        return sendingPacket;
//    }

//    private Boolean isMsgNotAcked(Integer senderId, Integer originalSenderId, Integer msgSeqNumber) {
////        LightMessage lightMessage = new LightMessage(senderId, originalSenderId, msgSeqNumber);
////        boolean isNotAcked = (ackedSet.get(senderId) == null || ! ackedSet.get(senderId).get(originalSenderId).contains(msgSeqNumber));
////        return isNotAcked;
//    }
    public void pl_send(String destIP, Integer destPort, Message msg) {
        String finalMsg = msg.getMsgContent() + "#" + msg.getOriginalSenderId() + "#" + msg.getSeqNumber();
        byte[] buf = finalMsg.getBytes();
        DatagramPacket msgPacket;
        try {
            msgPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }

        while(! ackedSet.contains(new LightMessage(host2IdMap.get(destIP).get(destPort), msg.getOriginalSenderId(), msg.getSeqNumber()))) {
            System.out.println(ackedSet);
            try {this.socket.send(msgPacket);} catch (IOException e) {throw new RuntimeException(e);}
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
//            System.out.println("pl-send");
        }
//            try {
//                Thread.sleep(300);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
    }

//    public void addToAckedSet(Integer senderId, Integer originalSenderId, Integer msgSeqNumber) {
//        if(isMsgNotAcked(senderId, originalSenderId, msgSeqNumber)) {
//            if (ackedSet.get(senderId) == null) {
//                ackedSet.put(senderId, new HashMap<>() {{
//                    put(originalSenderId, new HashSet<>() {{
//                        add(msgSeqNumber);
//                    }});
//                }});
//            } else {
//                ackedSet.get(senderId).get(originalSenderId).add(msgSeqNumber);
//            }
//        }
//    }

    private void pl_ack(String destIP, Integer destPort, Integer originalSenderId, Integer msgSeqNumber) {
        String ackMsg = "A" + "#" + originalSenderId + "#" + msgSeqNumber;
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
            String msg = new String(rcvPacket.getData(), 0, rcvPacket.getLength());

//            System.out.println("rcvd from " + senderIp + " " + senderPort + " : " + msg);

            int senderId = host2IdMap.get(senderIp).get(senderPort);

            String[] msgSplit = msg.split("#");

            // if the rcvd packet is an ack packet, add it to acked set
            Integer originalSenderId = Integer.parseInt(msgSplit[1]);
            Integer msgSeqNumber = Integer.parseInt(msgSplit[2]);

            // if it is an ack message
//            System.out.println(msgSplit[0]);
            LightMessage lightMessage = new LightMessage(senderId, originalSenderId, msgSeqNumber);
            if (msgSplit[0].equals("A")) {
                if(! ackedSet.contains(lightMessage))
                    ackedSet.add(lightMessage);
            }
            // else if it is a deliver msg, deliver it to the upper channel, and ack it
            else {
                pl_ack(senderIp, senderPort, originalSenderId, msgSeqNumber);
                pl_deliver(senderId, originalSenderId, msgSeqNumber, msgSplit[0]);
            }
        }
    }

    private synchronized void pl_deliver(Integer senderId, Integer originalSenderId, Integer msgSeqNumber, String msgContent) {
        LightMessage lightMessage = new LightMessage(senderId, originalSenderId, msgSeqNumber);
//        boolean isNotDelivered = deliveredSet.get(senderId) == null || ! deliveredSet.get(senderId).contains(msgSeqNumber);
        if(!deliveredSet.contains(lightMessage)) {
            // Deliver the message
            upperChannel.be_deliver(senderId, new Message(msgSeqNumber, originalSenderId, msgContent));
            deliveredSet.add(lightMessage);
            // Add sequence number to the delivered set
//            if (deliveredSet.get(senderId) == null) {
//                deliveredSet.put(senderId, new HashSet<>());
//                deliveredSet.get(senderId).add(msgSeqNumber);
//            } else {
//                deliveredSet.get(senderId).add(msgSeqNumber);
//            }
        }
    }

    private class LightMessage {
        public Integer getSenderId() {
            return senderId;
        }

        public Integer getOriginalSenderId() {
            return originalSenderId;
        }

        public Integer getSeqNumber() {
            return seqNumber;
        }

        private Integer senderId;
        private Integer originalSenderId;
        private Integer seqNumber;


        public LightMessage(Integer senderId, Integer originalSenderId, Integer seqNumber) {
            this.senderId = senderId;
            this.originalSenderId = originalSenderId;
            this.seqNumber = seqNumber;
        }

        @Override
        public boolean equals(Object that_) {
            if(that_ instanceof LightMessage) {
                LightMessage that = (LightMessage) that_;
                if(this.senderId == that.getSenderId()) {
                    if(this.originalSenderId == that.getOriginalSenderId()) {
                        if(this.seqNumber == that.getSeqNumber()) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public String toString()
        {
            return("s: " + senderId + " originId: " + originalSenderId + " sn: " + seqNumber);
        }
    }
}
