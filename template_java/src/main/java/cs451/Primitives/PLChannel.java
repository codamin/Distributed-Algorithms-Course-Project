package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.Ack;
import cs451.Primitives.Messages.Message;
import cs451.Primitives.Messages.Nack;
import cs451.Primitives.Messages.Proposal;

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
    private MapInt2Set ack_of_proposal_set = new MapInt2Set(); // store for each proc the set of proposals it has acked
    private MapInt2Set ack_of_ack_set = new MapInt2Set(); // store for each proc the set of Consensus Acks it has acked
    private MapInt2Set ack_of_nack_set = new MapInt2Set(); // store for each proc the set of Consensus Nacks it has acked

    private volatile LinkedBlockingQueue<DeliveryQueueInfo> deliverQueue = new LinkedBlockingQueue<>();
    private volatile LinkedBlockingQueue<SendingQueueInfo> resendingQueue= new LinkedBlockingQueue<>();
    private BEChannel upperChannel;

    public PLChannel(BEChannel beChannel, Host broadcaster, HashMap<String,
            HashMap<Integer, Integer>> host2IdMap) {
        this.broadcaster = broadcaster;
        this.host2IdMap = host2IdMap;
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
    }

    private void sendFromQueue() {
        while(true) {
//            try {
//                Thread.sleep(3);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
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
            try {
                 deliveryQueueInfo = deliverQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}
            addToDelivered(deliveryQueueInfo.message, deliveryQueueInfo.delivererId);
            upperChannel.be_deliver(deliveryQueueInfo.sourceIp, deliveryQueueInfo.sourcePort, deliveryQueueInfo.message);
        }
    }

    public void listen() {
        while(true) {
            byte[] rcvBuf = new byte[128];
            DatagramPacket rcvPacket = new DatagramPacket(rcvBuf, rcvBuf.length);

            // block to receive
            try {
                socket.receive(rcvPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String senderIp = rcvPacket.getAddress().getHostAddress();
            int senderPort = rcvPacket.getPort();

//            String rcvdMsg = new String(rcvPacket.getData(), 0, rcvPacket.getLength());
            byte[] rcvd_bytes = rcvPacket.getData();
            ByteBuffer buffer = ByteBuffer.wrap(rcvd_bytes, 0, rcvd_bytes.length);

            int senderId = host2IdMap.get(senderIp).get(senderPort);

//            System.out.println("rcvd msg:" + rcvdMsg + " from:" + senderId);

//            String[] msgSplit = rcvdMsg.split(" ");

            // if the rcvd packet is an ack packet, add it to acked set
//            char firstChar = rcvdMsg.charAt(0);

            int CHAR_SIZE = 2;
            int INT_SIZE = 4;

            char firstChar = buffer.getChar(0);

//            System.out.println(firstChar + " " + System.currentTimeMillis());

            Integer ackProposalNumber = buffer.getInt(CHAR_SIZE);
            // if it is a pl_ack for proposal message
            if(firstChar == 'a') {
                this.ack_of_ack_set.get(senderId).add(ackProposalNumber);
            }
            // if it is a pl_ack for nack message
            else if(firstChar == 'n') {
                this.ack_of_nack_set.get(senderId).add(ackProposalNumber);
            }
            // if it is a pl_ack for ack message
            else if(firstChar == '+') {
                this.ack_of_proposal_set.get(senderId).add(ackProposalNumber);
            }
            // if it is a message
            else if(firstChar == 'A' || firstChar == 'N' || firstChar == '@') {
                Integer proposal_number = buffer.getInt(CHAR_SIZE + INT_SIZE);
//                System.out.println("proposal_number: " + proposal_number + " " + System.currentTimeMillis());

                Message msg_to_be_delivred = null;
                Integer round = buffer.getInt(CHAR_SIZE);
                // Deliver if is not already + Acknowledge it
                if(firstChar == 'A') { // if it is a consensus ack :
                    msg_to_be_delivred = new Ack(round, proposal_number);
                }
                else if(firstChar == 'N' || firstChar == '@') {
                    int set_size = buffer.getInt(CHAR_SIZE + INT_SIZE + INT_SIZE);
                    HashSet set = new HashSet();
                    for(int i = 0; i < set_size; i++) {
                        int set_elem = buffer.getInt(CHAR_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + i*INT_SIZE);
                        set.add(set_elem);
                    }
//                    System.out.println(set_size + " " + set);
                    if(firstChar == 'N') { // if it is a consensus nack
                        msg_to_be_delivred = new Nack(round, proposal_number, set);
                    }
                    else if(firstChar == '@'){ // if it is a proposal
                        msg_to_be_delivred = new Proposal(round, proposal_number, set);
                    }
                }

                pl_ack(senderIp, senderPort, msg_to_be_delivred);

                if(! isDelivered(msg_to_be_delivred, senderId)) {
                    try {
                        deliverQueue.put(new DeliveryQueueInfo(senderId, msg_to_be_delivred, senderIp, senderPort));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}