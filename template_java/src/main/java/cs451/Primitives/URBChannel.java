package cs451.Primitives;

import cs451.Host;
import cs451.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    private BEChannel beChannel;

    private FIFOChannel upperChannel;

//    private HashSet<FIFOMessage> urb_deliveredSet = new HashSet<>();
    private boolean[][] urb_delivered2d;
//    private HashSet<FIFOMessage> urb_pendingSet = new HashSet<>();
    private boolean[][] urb_pending2d;
//    private HashMap<FIFOMessage, HashSet<Integer>> urb_ackedMap = new HashMap<>();
    private HashSet<Integer>[][] urb_ackedMap2d;

    public URBChannel(List<Host> hostsList, FIFOChannel fifoChannel, Host broadcaster, int NUMPROC, int NUMMSG) {
        this.urb_delivered2d = new boolean[NUMPROC][NUMMSG];
        this.urb_pending2d = new boolean[NUMPROC][NUMMSG];
        this.urb_ackedMap2d = new HashSet[NUMPROC][NUMMSG];
        this.hostsList = hostsList;
        this.upperChannel = fifoChannel;
        this.broadcaster = broadcaster;
        this.beChannel = new BEChannel(this.hostsList, this, broadcaster, NUMPROC, NUMMSG);
        this.beChannel.startThreads();
    }

    public void urb_broadcast(Integer broadcasterId, Message msg) {
//        System.out.println("urb broadcast...");
        // put the msg in broadcaster (as the original sender) pending list
//        this.urb_pendingSet.add(msg);
        urb_pending2d[msg.getOriginalSenderId()][msg.getSeqNumber()] = true;
        this.beChannel.be_broadcast(msg);
    }

    public void urb_deliver(Integer senderId, Message msg) {
        //************ ack[m] := ack[m] ∪ {p}; **************
        if(urb_ackedMap2d[msg.getOriginalSenderId()][msg.getSeqNumber()] == null) {
            urb_ackedMap2d[msg.getOriginalSenderId()][msg.getSeqNumber()] = new HashSet<>(){{add(senderId); add(broadcaster.getId());}};
        }
        else {
            urb_ackedMap2d[msg.getOriginalSenderId()][msg.getSeqNumber()].add(senderId);
            urb_ackedMap2d[msg.getOriginalSenderId()][msg.getSeqNumber()].add(broadcaster.getId());
        }

        if(! urb_pending2d[msg.getOriginalSenderId()][msg.getSeqNumber()]) {
            urb_pending2d[msg.getOriginalSenderId()][msg.getSeqNumber()] = true;
            beChannel.be_broadcast(msg);
        }
        checkAndDeliverToFiFo(senderId, msg);
    }

    private void checkAndDeliverToFiFo(Integer senderId, Message msg) {
        if(urb_ackedMap2d[msg.getOriginalSenderId()][msg.getSeqNumber()].size() > (this.hostsList.size()/2)) {
            if(! urb_delivered2d[msg.getOriginalSenderId()][msg.getSeqNumber()]) {
                System.out.println("urb delivering msg: " + msg);

                if(msg.getOriginalSenderId() == this.broadcaster.getId()) {
                    System.out.println("requesting next batch");
                    broadcaster.sendNextBatch();
                }
                urb_delivered2d[msg.getOriginalSenderId()][msg.getSeqNumber()] = true;
                upperChannel.fifo_deliver(msg);
            }
        }
    }
}
