package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    private BEChannel beChannel;

    private FIFOChannel upperChannel;

    private HashSet<FIFOMessage> urb_deliveredSet = new HashSet<>();
    private HashSet<FIFOMessage> urb_pendingSet = new HashSet<>();
    private HashMap<FIFOMessage, HashSet<Integer>> urb_ackedMap = new HashMap<>();

    public URBChannel(List<Host> hostsList, FIFOChannel fifoChannel, Host broadcaster) {
        this.hostsList = hostsList;
        this.upperChannel = fifoChannel;
        this.broadcaster = broadcaster;
        this.beChannel = new BEChannel(this.hostsList, this, broadcaster);
        this.beChannel.startThreads();
    }

    public void urb_broadcast(Integer broadcasterId, FIFOMessage msg) {
//        System.out.println("urb broadcast...");
        // put the msg in broadcaster (as the original sender) pending list
        this.urb_pendingSet.add(msg);
        this.beChannel.be_broadcast(msg);
    }

    public void urb_deliver(Integer senderId, FIFOMessage msg) {
        //************ ack[m] := ack[m] ∪ {p}; **************
        if(urb_ackedMap.get(msg) != null) {
            urb_ackedMap.get(msg).add(senderId);
//            urb_ackedMap.get(msg).add(broadcaster.getId());
        }
        else {
//            urb_ackedMap.put(msg, new HashSet<>(){{add(senderId); add(broadcaster.getId());
//            }});
            urb_ackedMap.put(msg, new HashSet<>(){{add(senderId);}});
        }
        if(! urb_pendingSet.contains(msg)) {
            urb_pendingSet.add(msg);
            beChannel.be_broadcast(msg);
        }
        checkAndDeliverToFiFo(senderId, msg);
    }

    private void checkAndDeliverToFiFo(Integer senderId, FIFOMessage msg) {
        if(urb_ackedMap.get(msg).size() > (this.hostsList.size()/2)) {
            if(! urb_deliveredSet.contains(msg)) {
                System.out.println("urb delivering msg: " + msg);

                if(msg.getOriginalSenderId() == this.broadcaster.getId()) {
                    System.out.println("requesting next batch");
                    broadcaster.sendNextBatch();
                }
//                else if(senderId == this.broadcaster.getId()) {
//                    System.out.println("requesting next batch");
//                    broadcaster.sendNextBatch();
//                }
                urb_deliveredSet.add(msg);
                upperChannel.fifo_deliver(msg);
            }
        }
    }
}
