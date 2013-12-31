import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;
import spouts.WordReader;

/**
 * User: domenicosolazzo
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException{
        // Topology definition
        TopologyBuilder topology =new TopologyBuilder();
        topology.setSpout("word-reader", new WordReader());
        topology.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        topology.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));


        // Configuration
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(true);

        // Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Submit Word count topology", conf, topology.createTopology());

        // Wait 2 seconds
        Thread.sleep(2000);
        // Shutdown
        cluster.shutdown();
    }

}
