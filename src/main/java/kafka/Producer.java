/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

    public class Producer {
        private final KafkaProducer<String, String> producer;
        private final String topic;

        public Producer(String topic) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "172.17.11.85:9092,172.17.11.86:9092,172.17.11.87:9092");
            props.put("client.id", "DemoProducer");
            props.put("batch.size", 16384);//16M
            props.put("linger.ms", 1000);
            props.put("buffer.memory", 33554432);//32M
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(props);
            this.topic = topic;
        }

        public void producerMsg() throws InterruptedException {
            String data = "Apache Storm is a free and open source distributed realtime computation system Storm makes it easy to reliably process unbounded streams of data doing for realtime processing what Hadoop did for batch processing. Storm is simple, can be used with any programming language, and is a lot of fun to use!\n" +
                    "Storm has many use cases: realtime analytics, online machine learning, continuous computation, distributed RPC, ETL, and more. Storm is fast: a benchmark clocked it at over a million tuples processed per second per node. It is scalable, fault-tolerant, guarantees your data will be processed, and is easy to set up and operate.\n" +
                    "Storm integrates with the queueing and database technologies you already use. A Storm topology consumes streams of data and processes those streams in arbitrarily complex ways, repartitioning the streams between each stage of the computation however needed. Read more in the tutorial.";
            data = data.replaceAll("[\\pP‘’“”]", "");
            String[] words = data.split(" ");
            Random _rand = new Random();

            Random rnd = new Random();
            int events = 10;
            for (long nEvents = 0; nEvents < events; nEvents++) {
                long runtime = new Date().getTime();
                int lastIPnum = rnd.nextInt(255);
                String ip = "192.168.2." + lastIPnum;
                String msg = words[_rand.nextInt(words.length)];
                try {
                    producer.send(new ProducerRecord<>(topic, ip, msg));
                    System.out.println("Sent message: (" + ip + ", " + msg + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Thread.sleep(10000);
        }
        public static void main(String[] args) throws InterruptedException {
            Producer producer = new Producer(Constants.TOPIC);
            producer.producerMsg();
        }
    }

