//package cs451.Primitives;
//
//import cs451.Host;
//import cs451.Message;
//
//import java.util.*;
//
//public class FIFOChannel {
//
//    private Host broadcaster;
//    private List<Host> hostsList;
//    private HashSet[] fifo_pendingPerSender;
//    private int[] next;
//    Integer lsn;
//    URBChannel urbChannel;
//
//    public FIFOChannel(List<Host> hostsList, Host broadcaster, int NUMPROC, int NUMMSG) {
//        this.next = new int[NUMPROC];
//        fifo_pendingPerSender = new HashSet[NUMPROC];
//        this.hostsList = hostsList;
//        this.lsn = 0;
//        for(Host dest_host : hostsList) {
//            next[dest_host.getId()]= 1;
//        }
//        this.broadcaster = broadcaster;
//        this.urbChannel = new URBChannel(this.hostsList, this, broadcaster, NUMPROC, NUMMSG);
//    }
//
//    public void fifo_broadcast() {
//        lsn += 1;
//        urbChannel.urb_broadcast(new Message(-1, broadcaster.getId(), lsn));
//    }
//
//    public void fifo_deliver(Message msg) {
//        //Add current msg to pending list --> pending := pending ∪ {(s, m, sn)};
//        Integer s = msg.getOriginalSenderId();
//
//        if(this.next[s] == msg.getSeqNumber()) {
//            broadcaster.deliver(next[s], s);
//            this.next[s] += 1;
//            while(this.fifo_pendingPerSender[s] != null && this.fifo_pendingPerSender[s].contains(next[s])) {
//                broadcaster.deliver(next[s], s);
//                this.fifo_pendingPerSender[s].remove(next[s]);
//                this.next[s] += 1;
//            }
//        }
//        else {
//            if(this.fifo_pendingPerSender[msg.getOriginalSenderId()] == null) {
//                this.fifo_pendingPerSender[msg.getOriginalSenderId()] = new HashSet<>(){{add(msg.getSeqNumber());}};
//            }
//            else {
//                this.fifo_pendingPerSender[msg.getOriginalSenderId()].add(msg.getSeqNumber());
//            }
//        }
//    }
//}
