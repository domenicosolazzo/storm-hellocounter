package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class WordCounter implements IRichBolt{
    private Integer id;
    private String name;
    Map<String, Integer> counters;
    private OutputCollector collector;

    /**
     * On create
     */
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }
    /**
     * On each word We will count
     */
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if(!counters.containsKey(word)){
            counters.put(word, 1);
        }else{
            Integer count = counters.get(word )+1;
            counters.put(word, count);
        }

        collector.ack(tuple);
    }

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     */
    @Override
    public void cleanup() {
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
