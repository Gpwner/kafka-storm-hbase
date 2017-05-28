package storm.topology;

import storm.bolt.WordCountBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;


public class KafkaWordCountHbaseTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if (args.length > 3) {
            TopologyBuilder builder = new TopologyBuilder();
            KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                    .builder(args[0], args[1])
                    .setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                    .setProp(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                    .setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)
                    .setOffsetCommitPeriodMs(10000)
                    .setGroupId(args[2])
                    .setMaxUncommittedOffsets(250)
                    .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
                    .build();
            SimpleHBaseMapper Mapper = new SimpleHBaseMapper()
                    .withRowKeyField("word")
                    .withColumnFields(new Fields("count"))
                    .withColumnFamily("result");
            HBaseBolt hbaseBolt = new HBaseBolt(args[3], Mapper)
                    .withConfigKey("hbase");
            KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);
            builder.setSpout("kafkaSpout", kafkaSpout, 1);
//            builder.setBolt("wordSplitBolt", new WordSplitBolt(), 2)
//                    .shuffleGrouping("kafkaSpout");
            builder.setBolt("countBolt", new WordCountBolt(), 2)
                    .fieldsGrouping("kafkaSpout", new Fields("value"));
            builder.setBolt("HbaseBolt", hbaseBolt, 1)
                    .addConfiguration("hbase", new HashMap<String, Object>())
                    .shuffleGrouping("countBolt");

            Config conf = new Config();
            conf.setDebug(true);
            if (args.length > 4) {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopologyWithProgressBar(args[4], conf, builder.createTopology());
            } else {
                conf.setMaxTaskParallelism(1);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("KafkaWordCountHbaseTopology", conf, builder.createTopology());
            }
        } else {
            System.out.println("参数列表 [bootstrapServers] [topics] [group_id] [HTable] [name]");
        }
    }
}
