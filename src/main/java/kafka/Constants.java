package kafka;

import java.util.Random;

/**
 * Created by root on 2017/5/23.
 */
public class Constants {
    public static final String TOPIC = "TOPIC-2019";
    public static final String GROUP = "GROUP-2019";

    public static void main(String[] args) {
        String data = "Apache Storm is a free and open source distributed realtime computation system Storm makes it easy to reliably process unbounded streams of data doing for realtime processing what Hadoop did for batch processing. Storm is simple, can be used with any programming language, and is a lot of fun to use!\n" +
                "Storm has many use cases: realtime analytics, online machine learning, continuous computation, distributed RPC, ETL, and more. Storm is fast: a benchmark clocked it at over a million tuples processed per second per node. It is scalable, fault-tolerant, guarantees your data will be processed, and is easy to set up and operate.\n" +
                "Storm integrates with the queueing and database technologies you already use. A Storm topology consumes streams of data and processes those streams in arbitrarily complex ways, repartitioning the streams between each stage of the computation however needed. Read more in the tutorial.";
        data = data.replaceAll("[\\pP‘’“”]", "");
        String[] words = data.split(" ");
        Random _rand = new Random();
        while (true)
        System.out.println(words[_rand.nextInt(words.length)]);

    }
}
