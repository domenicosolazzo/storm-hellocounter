package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class WordNormalizer implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }
    /**
     * The bolt will receive the line from the
     * words file and process it to Normalize this line
     *
     * The normalize will be put the words in lower case
     * and split the line to get all words in this
     */
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if( !word.isEmpty() ){
                word = word.toLowerCase();

                List a = new ArrayList();
                a.add(tuple);
                collector.emit(a, new Values(word));

            }
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
