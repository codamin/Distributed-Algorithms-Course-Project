package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.*;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;

    class MapInt2Set {
        HashMap<Integer, HashSet<Integer>> map = new HashMap<>();

        HashSet<Integer> get(Integer key) {
            if(! this.map.containsKey(key)) {
                this.map.put(key, new HashSet<>());
            }
            return this.map.get(key);
        }
    }
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;
    private MapInt2Set delivered_of_proposal_set = new MapInt2Set(); // store for each proc the set of proposals we have delivered
    private MapInt2Set delivered_of_ack_set = new MapInt2Set(); // store for each proc the set of Consensus Acks we have delivered
    private MapInt2Set delivered_of_nack_set = new MapInt2Set(); // store for each proc the set of Consensus Nacks we have delivered
    private MapInt2Set delivered_of_decision_set = new MapInt2Set(); // store for each proc the set of Consensus Decisions we have delivered

    private MapInt2Set ack_of_proposal_set = new MapInt2Set(); // store for each proc the set of proposals it has acked
    private MapInt2Set ack_of_ack_set = new MapInt2Set(); // store for each proc the set of Consensus Acks it has acked
    private MapInt2Set ack_of_nack_set = new MapInt2Set(); // store for each proc the set of Consensus Nacks it has acked
    private MapInt2Set ack_of_decision_set = new MapInt2Set(); // store for each proc the set of Consensus Decisions it has acked

    private volatile LinkedBlockingQueue<DeliveryQueueInfo> deliverQueue = new LinkedBlockingQueue<>();
    private volatile LinkedBlockingQueue<SendingQueueInfo> resendingQueue= new LinkedBlockingQueue<>();
    private BEChannel upperChannel;

    private int max_distinct_elems;
    private int NUM_PROC;

    public PLChannel(BEChannel beChannel, Host broadcaster, HashMap<String,
            HashMap<Integer, Integer>> host2IdMap, int NUM_PROC, int max_distinct_elems) {
        this.broadcaster = broadcaster;
        this.host2IdMap = host2IdMap;
        this.max_distinct_elems = max_distinct_elems;
        this.NUM_PROC = NUM_PROC;
        try {
            this.socket = new DatagramSocket(this.broadcaster.getPort());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        this.upperChannel = beChannel;
    }

    private boolean isAcked(Message message, Integer procId) {
        Boolean out = null;
        if(message instanceof Proposal) {
            out = this.ack_of_proposal_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Ack) {
            out = this.ack_of_ack_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Nack) {
            out = this.ack_of_nack_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Decided) {
            out = this.ack_of_decision_set.get(procId).contains(message.getRound());
        }
        return out;
    }

    private Boolean isDelivered(Message message, Integer procId) {
        Boolean out = null;
        if(message instanceof Proposal) {
            out = this.delivered_of_proposal_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Ack) {
            out = this.delivered_of_ack_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Nack) {
            out = this.delivered_of_nack_set.get(procId).contains(message.getProposal_number());
        }
        else if(message instanceof Decided) {
            out = this.delivered_of_decision_set.get(procId).contains(message.getRound());
//            System.out.println(out);
        }
//        System.out.println("delivered? : " + out);
        return out;
    }

    private void addToDelivered(Message message, Integer procId) {
        if(message instanceof Proposal) {
            this.delivered_of_proposal_set.get(procId).add(message.getProposal_number());
        }
        else if(message instanceof Ack) {
            this.delivered_of_ack_set.get(procId).add(message.getProposal_number());
        }
        else if(message instanceof Nack) {
            this.delivered_of_nack_set.get(procId).add(message.getProposal_number());
        }
        else if(message instanceof Decided) {
            this.delivered_of_decision_set.get(procId).add(message.getRound());
        }
    }

    private void sendFromQueue() {
        while(true) {
//            System.out.println(resendingQueue.size());
            SendingQueueInfo sendingQueueInfo = null;
            try {sendingQueueInfo = resendingQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}

//            System.out.println("sending: " + sendingQueueInfo.message.getProposal_number());
            DatagramPacket currentPacket = createSendingPacket(sendingQueueInfo.destIP, sendingQueueInfo.destPort, sendingQueueInfo.message.contentByteArray());
            Integer destId = host2IdMap.get(sendingQueueInfo.destIP).get(sendingQueueInfo.destPort);

            if(! isAcked(sendingQueueInfo.message, destId)) {
                try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
//                System.out.println("sening msg: " + sendingQueueInfo.message);
                try {resendingQueue.put(sendingQueueInfo);} catch (InterruptedException e) {throw new RuntimeException(e);}
            }
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
        public Message message;
//        public HashSet ;
        public SendingQueueInfo(String destIP, Integer destPort, Message message) {
            this.destIP = destIP;
            this.destPort = destPort;
            this.message = message;
        }
    }

    private class DeliveryQueueInfo {
        public Integer delivererId;
        public Message message;
        public String sourceIp;
        public Integer sourcePort;
        public DeliveryQueueInfo(Integer delivererId, Message message, String sourceIp, Integer sourcePort) {
            this.delivererId = delivererId;
            this.message = message;
            this.sourceIp = sourceIp;
            this.sourcePort = sourcePort;
        }
    }

    private DatagramPacket createSendingPacket(String destIP, Integer destPort, byte[] msg_bytes) {
        byte[] buf = msg_bytes;
        DatagramPacket msgPacket;
        try {
            msgPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
        } catch (UnknownHostException e) {
            System.out.println("The given host name is unknown.");
            throw new RuntimeException(e);
        }
        return msgPacket;
    }
    public void pl_send(String destIP, Integer destPort, Message message) {
        // send it once yourself
        SendingQueueInfo sendingQueueInfo = new SendingQueueInfo(destIP, destPort, message);
        // then add it to resend queue which will check and resend
        try {
            resendingQueue.put(sendingQueueInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void pl_ack(String destIP, Integer destPort, Message message) {
//        byte[] buf = message.getAckMsg().getBytes();
        byte[] buf = message.ackByteArray();
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
            DeliveryQueueInfo deliveryQueueInfo;
            // block to find msg
            try {deliveryQueueInfo = deliverQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}
            if(! isDelivered(deliveryQueueInfo.message, deliveryQueueInfo.delivererId)) {
                addToDelivered(deliveryQueueInfo.message, deliveryQueueInfo.delivererId);
                upperChannel.be_deliver(deliveryQueueInfo.sourceIp, deliveryQueueInfo.sourcePort, deliveryQueueInfo.message);
            }
        }
    }

    public void listen() {
        while(true) {
            byte[] rcvBuf = new byte[(NUM_PROC * max_distinct_elems)*4 + 100];
            DatagramPacket rcvPacket = new DatagramPacket(rcvBuf, rcvBuf.length);

            // block to receive
            try {
                socket.receive(rcvPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String senderIp = rcvPacket.getAddress().getHostAddress();
            int senderPort = rcvPacket.getPort();

            byte[] rcvd_bytes = rcvPacket.getData();
            ByteBuffer buffer = ByteBuffer.wrap(rcvd_bytes);
//            buffer.rewind();
            int senderId = host2IdMap.get(senderIp).get(senderPort);

            int CHAR_SIZE = 2;
            int INT_SIZE = 4;

            char firstChar = buffer.getChar();

//            System.out.println(firstChar + " " + System.currentTimeMillis());
            // if it is a pl_ack for proposal message
            if(firstChar == 'a') {
                this.ack_of_ack_set.get(senderId).add(buffer.getInt());
            }
            // if it is a pl_ack for nack message
            else if(firstChar == 'n') {
                this.ack_of_nack_set.get(senderId).add(buffer.getInt());
            }
            // if it is a pl_ack for ack message
            else if(firstChar == '+') {
                this.ack_of_proposal_set.get(senderId).add(buffer.getInt());
            }
            // if it is a pl_ack for decision message
            else if(firstChar == 'd') {
                this.ack_of_decision_set.get(senderId).add(buffer.getInt());
            }
            // if it is a message
            else if(firstChar == 'A' || firstChar == 'N' || firstChar == '@' || firstChar == 'D') {
//                Integer proposal_number = buffer.getInt();
//                System.out.println("proposal_number: " + proposal_number + " " + System.currentTimeMillis());

                Message msg_to_be_delivred = null;

                if (firstChar == 'D') {
                    Integer round = buffer.getInt();
                    msg_to_be_delivred = new Decided(round);
                }
                // Deliver if is not already + Acknowledge it
                else if(firstChar == 'A') { // if it is a consensus ack :
                    Integer round = buffer.getInt();
                    Integer proposal_number = buffer.getInt();
                    msg_to_be_delivred = new Ack(round, proposal_number);
                }
                else if(firstChar == 'N' || firstChar == '@') {
                    Integer round = buffer.getInt();
                    Integer proposal_number = buffer.getInt();
                    Integer set_size = buffer.getInt();
//                    System.out.println(set_size);
//                    System.out.println(firstChar + " " + round + " " + proposal_number + " " + set_size + " " + buffer.limit());
                    HashSet<Integer> set = new HashSet();
                    for(int i = 0; i < set_size; i++) {
                        Integer set_elem = buffer.getInt();
                        set.add(set_elem);
                    }
                    if(firstChar == 'N') { // if it is a consensus nack
                        msg_to_be_delivred = new Nack(round, proposal_number, set);
                    }
                    else if(firstChar == '@'){ // if it is a proposal
                        msg_to_be_delivred = new Proposal(round, proposal_number, set);
                    }
                }

                pl_ack(senderIp, senderPort, msg_to_be_delivred);
                 try {
                        deliverQueue.put(new DeliveryQueueInfo(senderId, msg_to_be_delivred, senderIp, senderPort));
                 } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                }
            }
        }
    }
}