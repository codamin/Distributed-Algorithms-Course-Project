package cs451.Primitives;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class Application {

    private String outputPath;

    public Application(String outputPath_) {
        outputPath = outputPath_;
    }

    public void writeLogs2Output() {
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        writer.println(logs.logString);
        writer.close();
    }

    public void log(String typeOfOperation, Integer senderId, Integer msgSeqNumber) {
        if(typeOfOperation.equals("d")) {
//            System.out.println("adding log to output 2");
            logs.addLog(typeOfOperation + " " + senderId + " " + msgSeqNumber.toString());
//            System.out.println("logs = " + logs.logString);
        }
        else if(typeOfOperation.equals("b")) {
            logs.addLog(typeOfOperation + " " + msgSeqNumber.toString());
        }
    }

    class Logs {
        public String logString = "";
        public synchronized void addLog(String addition) {
            logString += addition;
            logString += "\n";
        }
    }
    final Logs logs = new Logs();
}
