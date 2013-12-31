package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class WordReader implements IRichSpout{
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;

    public boolean isDistributed() {return false;}

    /**
     * Declare an output field "line"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    /**
     * It will create a file and get the collector object
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        try{
            this.context = topologyContext;
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        }catch(FileNotFoundException e){

            throw new RuntimeException("Error reading the file [" + conf.get("wordsFile") +"]");
        }


        this.collector = collector;
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        if(completed){
            try{
                Thread.sleep(1);// Wait a second
            }catch(InterruptedException e){
                // Do nothing

            }

        }

        String str;

        BufferedReader reader = new BufferedReader(fileReader);
        try{
            // Read all the lines
            while( (str = reader.readLine()) != null ){
                /**
                 *  Read all the line and emit them to the bolts
                 */
                this.collector.emit(new Values(str), str);
            }
        }catch(Exception ex){
            throw new RuntimeException("Error parsing the tuple: " + ex);
        }finally{
            completed = true;
        }
    }

    @Override
    public void ack(Object o) {
        System.out.println("OK: " + o);
    }

    @Override
    public void fail(Object o) {
        System.out.println("Fail: " + o);
    }
}
