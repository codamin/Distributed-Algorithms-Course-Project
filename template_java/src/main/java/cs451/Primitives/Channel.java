//package cs451.Primitives;
//
//import cs451.FIFOMessage;
//import cs451.Host;
//import cs451.PLMessage;
//
//import java.io.IOException;
//import java.net.DatagramPacket;
//import java.net.DatagramSocket;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.security.KeyPair;
//import java.util.*;
//import java.util.concurrent.ArrayBlockingQueue;
//
//import static java.lang.Math.min;
//
//public class Channel {
//
//    Integer numOfMsg;
//    Host broadcaster;
//    List<Host> hostList;
//    /////////////////////////////////////////////////////////////////////////////////////
//    private Integer fifo_lsn;
//    private ArrayList<FIFOMessage> sendingArray = new ArrayList<>();
//    private ArrayList<FIFOMessage> fifo_pendingList = new ArrayList<>();
//    private HashMap<Integer, Integer> fifo_next = new HashMap<>();
//    /////////////////////////////////////////////////////////////////////////////////////
//    private HashSet<FIFOMessage> urb_deliveredSet = new HashSet<>();
//    private HashSet<FIFOMessage> urb_pendingSet = new HashSet<>();
//    private HashMap<FIFOMessage, HashSet<Integer>> urb_ackedMap = new HashMap<>();
//    /////////////////////////////////////////////////////////////////////////////////////
//    private DatagramSocket pl_socket;
//    private HashMap<String, HashMap<Integer, Integer>> pl_host2IdMap;
//    private ArrayList<KeyPair>resendList<>
//    private HashSet<PLMessage> pl_deliveredSet = new HashSet<>();
//    private HashSet<PLMessage> pl_ackedSet = new HashSet<>();
//    private Queue<PLMessage> pl_deliverQueue = new LinkedList<>();
//    private Queue<String> pl_deliverQueue_msgContent = new LinkedList<>();
//
//    int SENDING_QUEUE_CAPACITY = 1000;
//    private ArrayBlockingQueue<DatagramPacket> sendingQueue= new ArrayBlockingQueue<>(SENDING_QUEUE_CAPACITY);
//
//    private ArrayBlockingQueue<PLMessage> sendingQueueMsg= new ArrayBlockingQueue<>(SENDING_QUEUE_CAPACITY);
//    /////////////////////////////////////////////////////////////////////////////////////
//    public Channel(List<Host> hostsList, Host broadcaster, Integer numOfMsg) {
//        this.numOfMsg = numOfMsg;
//        this.broadcaster = broadcaster;
//
//        fifo_lsn = 0;
//        for(Host dest_host : hostsList) {
//            fifo_next.put(dest_host.getId(), 1);
//        }
//
//        for(Host host: hostsList) {
//            if(pl_host2IdMap.get(host.getIp()) == null) {
//                pl_host2IdMap.put(host.getIp(), new HashMap<>() {{put(host.getPort(), host.getId());}});
//            }
//            else {
//                pl_host2IdMap.get(host.getIp()).put(host.getPort(), host.getId());
//            }
//        }
//    }
//
//    public void fifo_broadcast() {
//        int i = 0;
//        while(i <= numOfMsg) {
//            ArrayList<FIFOMessage> sendingList;
//            for(int z = 0; z < 1000; z++) {
//                if(i > numOfMsg) {
//                    break;
//                }
//                String msgString = "";
//                for (int j = i; j <= min(numOfMsg, i + 7); j++) {
//                    this.broadcaster.getApplicationLayer().log("b", null, j);
//                    msgString += Integer.toString(j);
//                    msgString += ",";
//                }
//                i = i + 8;
//                //#########################
//                fifo_lsn += 1;
//                FIFOMessage fifoMsg = new FIFOMessage(fifo_lsn, broadcaster.getId(), msgString);
//                //#########################
//                this.urb_pendingSet.add(fifoMsg);
//                this.sendingArray.add(fifoMsg);
//            }
////#########################
//            for(Host host: this.hostList) {
////            System.out.println("broadcasting to: " + host.getId());
//                pl_send(host.getIp(), host.getPort());
//            }
//        }
//    }
//
//    public void pl_send(String destIP, Integer destPort) {
//        Iterator<FIFOMessage> iterator = sendingArray.iterator();
//
//        while(iterator.hasNext()) {
//            FIFOMessage fifoMsg = iterator.next();
//            String finalMsg = fifoMsg.getMsgContent() + "#" + fifoMsg.getOriginalSenderId() + "#" + fifoMsg.getSeqNumber();
//            byte[] buf = finalMsg.getBytes();
//            DatagramPacket sendingPacket;
//            try {
//                sendingPacket = new DatagramPacket(buf, buf.length, InetAddress.getByName(destIP), destPort);
//            } catch (UnknownHostException e) {
//                System.out.println("The given host name is unknown.");
//                throw new RuntimeException(e);
//            }
//
//            // create a msg with sender=dest to see if it is acked or not
//            PLMessage sendingPLMsg = new PLMessage(pl_host2IdMap.get(destIP).get(destPort), fifoMsg.getOriginalSenderId(), fifoMsg.getSeqNumber());
//
//            if(! pl_ackedSet.contains(sendingPLMsg)) {
//                try {this.pl_socket.send(sendingPacket);} catch (IOException e) {throw new RuntimeException(e);}
//            }
//            else {
//                resendList.add(new Pair<>(sendingPacket, sendingPLMsg));
//            }
//        }
//    }
//
//    private void sendFromQueue() {
//        while(true) {
//            DatagramPacket currentPacket = sendingQueue.peek();
//            PLMessage currentMessage = sendingQueueMsg.peek();
//
//            System.out.println(sendingQueue.size() + "," + sendingQueueMsg.size());
//
//            if((currentPacket != null) && (currentMessage != null) && (sendingQueue.size() == sendingQueueMsg.size())) {
//                long t0 = System.nanoTime();
//                while(! pl_ackedSet.contains(currentMessage)) {
//                    long t1 = System.nanoTime();
//                    if(t1-t0 > 100 * 1000) {
//                        sendingQueue.put(sendingQueue.poll());
//                        sendingQueueMsg.put(sendingQueueMsg.poll());
//                    }
//                    try {this.pl_socket.send(currentPacket);} catch (IOException e) {throw new RuntimeException(e);}
//                }
////                System.out.println(Thread.currentThread().getName() + ": sent msg: " + currentMessage);
//            }
//        }
//    }
//}
