package cs451.Primitives;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class PLChannel {

    private Host broadcaster;
    private DatagramSocket socket;
    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;
    private boolean[][][] delivered3d;
    private boolean[][][] acked3d;
    private volatile LinkedBlockingQueue<Message> deliverQueue = new LinkedBlockingQueue<>();

    private volatile LinkedBlockingQueue<SendingQueueInfo> resendingQueue= new LinkedBlockingQueue<>();

    private BEChannel upperChannel;

    private Boolean busy = false;

    public PLChannel(BEChannel beChannel, Host broadcaster, HashMap<String,
            HashMap<Integer, Integer>> host2IdMap, int NUMPROC, int NUMMSG) {
        this.delivered3d = new boolean[NUMPROC][NUMPROC][NUMMSG];
        this.acked3d = new boolean[NUMPROC][NUMPROC][NUMMSG];
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
            SendingQueueInfo sendingQueueInfo = null;
            try {sendingQueueInfo = resendingQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}

            DatagramPacket currentPacket = createSendingPacket(sendingQueueInfo.destIP, sendingQueueInfo.destPort, sendingQueueInfo.fifoMsg);
            Integer destId = host2IdMap.get(sendingQueueInfo.destIP).get(sendingQueueInfo.destPort);

            if(!acked3d[destId][sendingQueueInfo.fifoMsg.getOriginalSenderId()][sendingQueueInfo.fifoMsg.getSeqNumber()]) {
                try {this.socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
//                System.out.println("send msg: " + sendingQueueInfo.fifoMsg);
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
        public Message fifoMsg;

        public SendingQueueInfo(String destIP, Integer destPort, Message fifoMsg) {
            this.destIP = destIP;
            this.destPort = destPort;
            this.fifoMsg = fifoMsg;
        }
    }

    private DatagramPacket createSendingPacket(String destIP, Integer destPort, Message fifoMsg) {
        String finalMsg = fifoMsg.getOriginalSenderId() + "#" + fifoMsg.getSeqNumber();
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
    public void pl_send(String destIP, Integer destPort, Message fifoMsg) {
        // send it once yourself
        SendingQueueInfo sendingQueueInfo = new SendingQueueInfo(destIP, destPort, fifoMsg);
        // then add it to resend queue which will check and resend
        try {
            resendingQueue.put(sendingQueueInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void pl_ack(String destIP, Integer destPort, Integer originalSenderId, Integer msgSeqNumber) {
        String ackMsg = originalSenderId + "#" + msgSeqNumber + "#" + "A";
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
            Message msg;
            // block to find msg
            try {msg = deliverQueue.take();} catch (InterruptedException e) {throw new RuntimeException(e);}
            delivered3d[msg.getSenderId()][msg.getOriginalSenderId()][msg.getSeqNumber()] = true;
            upperChannel.be_deliver(msg);
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
            String rcvdMsg = new String(rcvPacket.getData(), 0, rcvPacket.getLength());
            int senderId = host2IdMap.get(senderIp).get(senderPort);

//            System.out.println("rcvd msg:" + rcvdMsg + " from:" + senderId);

            String[] msgSplit = rcvdMsg.split("#");

            // if the rcvd packet is an ack packet, add it to acked set
            Integer originalSenderId = Integer.parseInt(msgSplit[0]);
            Integer msgSeqNumber = Integer.parseInt(msgSplit[1]);

            // if it is an ack message
            if(msgSplit.length == 3) {
                acked3d[senderId][originalSenderId][msgSeqNumber] = true;
            }
            // else if it is a deliver msg, deliver it to the upper channel, and ack it
            else {
                Message msg = new Message(senderId, originalSenderId, msgSeqNumber);
                pl_ack(senderIp, senderPort, originalSenderId, msgSeqNumber);
                if(! delivered3d[senderId][originalSenderId][msgSeqNumber] && ! deliverQueue.contains(msg)) {
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