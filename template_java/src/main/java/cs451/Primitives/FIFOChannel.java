package cs451.Primitives;

import cs451.Host;
import cs451.Message;

import java.util.*;

public class FIFOChannel {

    private Host broadcaster;
    private String SEQ_DELIM = "#";
    private List<Host> hostsList;

    //Map Process --> Array of pending msgs
//    private ArrayList<Message> fifo_pendingList = new ArrayList<>();
    private HashSet[] fifo_pendingPerSender = new HashSet[128];
//    private HashMap<Integer, Integer> next = new HashMap<>();
    private int[] next;
    Integer lsn;
    URBChannel urbChannel;

    public FIFOChannel(List<Host> hostsList, Host broadcaster, int NUMPROC, int NUMMSG) {
        this.next = new int[NUMPROC];
        fifo_pendingPerSender = new HashSet[NUMPROC];
        this.hostsList = hostsList;
        this.lsn = 0;
        for(Host dest_host : hostsList) {
            next[dest_host.getId()]= 1;
        }
        this.broadcaster = broadcaster;
        this.urbChannel = new URBChannel(this.hostsList, this, broadcaster, NUMPROC, NUMMSG);
    }

    public void fifo_broadcast() {
        lsn += 1;
        urbChannel.urb_broadcast(this.broadcaster.getId(), new Message(lsn, broadcaster.getId()));
    }

    public void fifo_deliver(Message msg) {
        //Add current msg to pending list --> pending := pending ∪ {(s, m, sn)};
        Integer s = msg.getOriginalSenderId();

        if(this.next[s] == msg.getSeqNumber()) {
            broadcaster.deliver(next[s], s);
            this.next[s] += 1;
            while(this.fifo_pendingPerSender[s] != null && this.fifo_pendingPerSender[s].contains(next[s])) {
                broadcaster.deliver(next[s], s);
                this.fifo_pendingPerSender[s].remove(next[s]);
                this.next[s] += 1;
            }
        }
        else {
            if(this.fifo_pendingPerSender[msg.getOriginalSenderId()] == null) {
                this.fifo_pendingPerSender[msg.getOriginalSenderId()] = new HashSet<>(){{add(msg.getSeqNumber());}};
            }
            else {
                this.fifo_pendingPerSender[msg.getOriginalSenderId()].add(msg.getSeqNumber());
            }
        }

//        this.fifo_pendingList.add(msg);
        //Deliver previous messages
//        boolean IsPrevRemaining = true;
//        while(IsPrevRemaining) {
//            IsPrevRemaining = false;

            //loop over messages
//            Iterator<Message> iterator = this.fifo_pendingList.iterator();
//            while(iterator.hasNext()) {
//                Message pendingMsg = iterator.next();
//
//                if(s == pendingMsg.getOriginalSenderId()) {
//                    if(pendingMsg.getSeqNumber() == this.next[s]) {
//                        IsPrevRemaining = true;
//                        this.next[s] = this.next[s] + 1;
//                        iterator.remove();
//                        System.out.println("content: " + pendingMsg.getSeqNumber());
//                        broadcaster.deliver(pendingMsg.getSeqNumber(), msg.getOriginalSenderId());
//                        //////////////////
////                        pendingMsg.wipe();
//                        //////////////////
//                    }
//                }
//            }

//        }
    }
}
