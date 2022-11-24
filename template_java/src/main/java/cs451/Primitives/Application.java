package cs451.Primitives;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class Application {

    private String outputPath;

    private Integer lineCapacity = 1000;
    private Integer numLines = 0;

    PrintWriter writer;

    public Application(String outputPath_) {
        outputPath = outputPath_;
    }

    public void writeLogs2Output() {
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        writer.print(logs.logString);
        writer.close();
    }

    public synchronized void log(String typeOfOperation, Integer senderId, Integer msgSeqNumber) {
        if(numLines.equals(lineCapacity)) {
            this.flush();
            numLines = 0;
        }
        numLines += 1;
        if(typeOfOperation.equals("d")) {
            logs.addLog(typeOfOperation + " " + senderId + " " + msgSeqNumber.toString());
        }
        else if(typeOfOperation.equals("b")) {
            logs.addLog(typeOfOperation + " " + msgSeqNumber.toString());
        }
    }
    private void flush() {
        try {
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), true));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        writer.print(logs.logString);
        writer.close();
        this.logs.empty();
    }
    class Logs {
        public String logString = "";

        public void empty() {
            this.logString = "";
        }
        public synchronized void addLog(String addition) {
            logString += addition;
            logString += "\n";
        }
    }
    final Logs logs = new Logs();
}
