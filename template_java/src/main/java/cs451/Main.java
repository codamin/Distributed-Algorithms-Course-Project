package cs451;

import cs451.Primitives.Application;

import java.io.*;
import java.util.HashMap;
import java.util.Scanner;

public class Main {

    static Host thisHost;
    static Application applicationLayer;
    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary

        if (thisHost != null) {
            System.out.println("Writing output.");
            applicationLayer.writeLogs2Output();
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello");
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        File myObj = new File(parser.config());
        Scanner myReader;
        try {
            myReader = new Scanner(myObj);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        int numOfMsg = 0;
        while (myReader.hasNextLine()) {
            String[] parsedLine = myReader.nextLine().split(" ");
            numOfMsg = Integer.parseInt(parsedLine[0]);
        }

        System.out.println("num of msg = " + numOfMsg);

        System.out.println("Broadcasting and delivering messages...\n");

        // create ip,port --> id map
        HashMap<String, Integer> host2IdMap = new HashMap<>();
        for(Host host_: parser.hosts()) {
            host2IdMap.put(host_.getIp() + ":" + host_.getPort(), host_.getId());
        }

        // Set host2IdMap in each host
        for(Host host_: parser.hosts()) {
            host_.setHost2IdMap(host2IdMap);
        }
        
        // Set Hosts' output paths
        for(Host host_: parser.hosts()) {
            host_.setApplicationLayer(applicationLayer);
        }
        applicationLayer = new Application(parser.output());

        // find the host object corresponding to the current process
        for(Host host_: parser.hosts()) {
            if(host_.getId() == parser.myId())
                thisHost = host_;
        }

        thisHost.setHosts(parser.hosts());
        thisHost.start(numOfMsg);

         // After a process finishes broadcasting,
         // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
