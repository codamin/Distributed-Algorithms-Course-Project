package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;
import cs451.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    private BEChannel beChannel;

    private FIFOChannel upperChannel;

    HashSet<Message> urb_deliveredSet = new HashSet<>();
    HashSet<Message> urb_pendingSet = new HashSet<>();
    HashMap<Message, HashSet<Integer>> urb_ackedMap = new HashMap<>();

    public URBChannel(List<Host> hostsList, FIFOChannel fifoChannel, Host broadcaster) {
        this.hostsList = hostsList;
        this.beChannel = new BEChannel(this.hostsList, this, broadcaster);
        this.upperChannel = fifoChannel;
        this.broadcaster = broadcaster;
    }

    public void urb_broadcast(Integer broadcasterId, FIFOMessage msg) {
        System.out.println("urb broadcast...");
        // put the msg in broadcaster (as the original sender) pending list
        System.out.println(broadcasterId);
        this.urb_pendingSet.add(msg);
        this.beChannel.be_broadcast(msg);
    }

    public void urb_deliver(Integer senderId, FIFOMessage msg) {

        //************ ack[m] := ack[m] ∪ {p}; **************
        ///////////////////////////////////////////
        System.out.println("in urb delivery...");

        if(urb_ackedMap.get(msg.getSeqNumber()) != null) {
            urb_ackedMap.get(msg.getSeqNumber()).add(senderId);
        }
        else {
            urb_ackedMap.put(msg, new HashSet<>(){{add(senderId);}});
        }
        ///////////////////////////////////////////
        //************ if (s, m) is not pending then
        // pending := pending ∪ {(s, m)};
        // trigger < beb, Broadcast | [DATA, s, m] >; **************
        ///////////////////////////////////////////
        if(! urb_pendingSet.contains(msg)) {
            System.out.println("Relaying msg: " + msg + " sent from " + senderId);

            urb_pendingSet.add(msg);
             //relay message
            beChannel.be_broadcast(msg);
//           // deliver if can deliver
//            System.out.println("calling checkAndDeliverToFiFo:");
            checkAndDeliverToFiFo(msg);
        }
        else {
            System.out.println("Did not relay msg from " + senderId + " ---> already in pending list");
        }
        System.out.println("urb_pendingSet:" + urb_pendingSet);
    }

    private void checkAndDeliverToFiFo(FIFOMessage msg) {
        System.out.println("urb_ackedMap.get(msg).size() : " + urb_ackedMap.get(msg).size());
        if(urb_ackedMap.get(msg).size() > (this.hostsList.size()+1)/2) {
            if(! urb_deliveredSet.contains(msg.getSeqNumber())) {
                urb_deliveredSet.add(msg);
                System.out.println("delivered to fifo channel...................");
                upperChannel.fifo_deliver(msg);
            }
        }
    }
}
