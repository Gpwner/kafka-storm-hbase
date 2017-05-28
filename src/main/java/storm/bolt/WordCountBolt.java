package storm.bolt;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class WordCountBolt extends BaseBasicBolt {
    private Map<String, Integer> counts = new HashMap<>();

    public void execute(Tuple input, BasicOutputCollector collector) {
        String level = input.getStringByField("value");
        Integer count = counts.get(level);
        if (count == null)
            count = 0;
        count++;
        counts.put(level, count);
        System.out.println("WordCountBolt Receive : "+level+"   "+count);
        collector.emit(new Values(level, count.toString()));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
