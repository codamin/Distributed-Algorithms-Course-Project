package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class FIFOChannel {

    private Host broadcaster;
    private String SEQ_DELIM = "#";
    private List<Host> hostsList;

    //Map Process --> Array of pending msgs
    private ArrayList<FIFOMessage> fifo_pendingList = new ArrayList<>();
    private HashMap<Integer, Integer> next = new HashMap<>();
    Integer lsn;
    URBChannel urbChannel;

    public FIFOChannel(List<Host> hostsList, Host broadcaster) {
        this.hostsList = hostsList;
        this.lsn = 0;
        for(Host dest_host : hostsList) {
            next.put(dest_host.getId(), 1);
        }
        this.broadcaster = broadcaster;
        this.urbChannel = new URBChannel(this.hostsList, this, broadcaster);
    }

    public void fifo_broadcast(String msg) {
//        System.out.println("fifo broadcast...");
        lsn += 1;
//        this.broadcaster.getApplicationLayer().log("b", null, lsn);
        urbChannel.urb_broadcast(this.broadcaster.getId(), new FIFOMessage(lsn, broadcaster.getId(), msg));
//        System.out.println("fifo finished........");
    }

    public void fifo_deliver(FIFOMessage msg) {
//        broadcaster.sendNextBatch();
        //Add current msg to pending list --> pending := pending ∪ {(s, m, sn)};
        Integer s = msg.getOriginalSenderId();
        this.fifo_pendingList.add(msg);
        //Deliver previous messages
//        ArrayList<String> removePendingBuffer = new ArrayList<>();

        boolean IsPrevRemaining = true;
        while(IsPrevRemaining) {
            IsPrevRemaining = false;

            //loop over messages
            Iterator<FIFOMessage> iterator = this.fifo_pendingList.iterator();
            while(iterator.hasNext()) {
                FIFOMessage pendingMsg = iterator.next();

                if(s == pendingMsg.getOriginalSenderId()) {
                    if (pendingMsg.getSeqNumber() == this.next.get(s)) {
                        IsPrevRemaining = true;
                        //next[s] := next[s] + 1;
                        this.next.put(s, this.next.get(s) + 1);
                        //pending := pending \ {(s, m, sn)};
                        iterator.remove();
                        //trigger < frb, Deliver | s, m >;
                        System.out.println("content: " + pendingMsg.getMsgContent());
                        String[] msgsplit = pendingMsg.getMsgContent().split(",");
                        for(int i = 0; i < msgsplit.length; i++) {
                            this.broadcaster.getApplicationLayer().log("d", s, Integer.parseInt(msgsplit[i]));
                        }
                        //////////////////
//                        pendingMsg.wipe();
                        //////////////////
                    }
                }
            }
        }
    }
}
