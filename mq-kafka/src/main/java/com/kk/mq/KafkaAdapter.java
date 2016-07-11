package com.kk.mq;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 使用时候 需要指定 是线上环境还是测试环境。  默认线上环境。
 */
public class KafkaAdapter {
    private static Log logger = LogFactory.getLog(KafkaAdapter.class);

    private static KafkaAdapter _instance = null;

    private String zk_hosts = null;
    private String kafka_hosts = null;
    private Producer<String, String> producer = null;


    // group
    public static final String GROUP_WALLET = "group";


    // topic
    public static String TOPIC_LOGIN = "login"; // 钱包 app登录后通知




    // 设置测试环境和线上环境， 默认线上环境。
    public static void setTestEnvironment(boolean test) {
        if (test) {
            System.setProperty("com.kk.mq.kafka.environment", "1");
        } else {
            System.setProperty("com.kk.mq.kafka.environment", "0");
        }
    }

    private KafkaAdapter() {
        try {
            init();

            createProducer();


        } catch (Exception e) {
            logger.error("get client error", e);
        }
    }

    private void init() throws IOException {
        InputStream in = KafkaAdapter.class.getResourceAsStream("kafka.properties");
        Properties prop = new Properties();
        prop.load(in);

        String environment = System.getProperty("com.kk.mq.kafka.environment");
        if (environment != null && "1".equals(environment)) {
            zk_hosts = (String) prop.get("zk_hosts_test");
            kafka_hosts = (String) prop.get("kafka_hosts_test");

            logger.info("kafka.environment is test now");
        } else {
            zk_hosts = (String) prop.get("zk_hosts");
            kafka_hosts = (String) prop.get("kafka_hosts");

            logger.info("kafka.environment is online now");
        }

        if (zk_hosts == null || kafka_hosts == null) {
            throw new RuntimeException("Need conf for zookeeper zk_hosts.");
        }
    }

    private void createProducer() {
        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", kafka_hosts);
        //StringEncoder 默认编码为utf8
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
//                    props.put("partitioner.class", "com.kk.mq.PartitionerDemo");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        producer = new Producer<String, String>(config);
    }


    public Producer<String, String> getProducer() {
        return producer;
    }

    public String getZk_hosts() {
        return zk_hosts;
    }

    public String getKafka_hosts() {
        return kafka_hosts;
    }

    public static KafkaAdapter getInstance() {
        if (_instance == null) {
            synchronized (KafkaAdapter.class) {
                if (_instance == null) {
                    _instance = new KafkaAdapter();
                }
            }
        }
        return _instance;
    }

    // partition为0， 仅1个分区
    public void sendMessage(String topic, String msg) {
        try {
            // 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            //  可以设置 key,  同时配置上 partition  分发策略。
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
            producer.send(data);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    // 默认 1个 分区
    public void test_consume(String group, String topic) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(
                zk_hosts, group));

        int partitionNum = 1;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, Integer.valueOf(partitionNum));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        while (it.hasNext()) {
            try {
                System.out.println(new String(it.next().message(), "utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }

    // 默认 1个 分区
    public void consume(String group, String topic, KafkaConsume consume) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(
                zk_hosts, group));

        int partitionNum = 1;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, Integer.valueOf(partitionNum));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        while (it.hasNext()) {
            try {
                consume.run(new String(it.next().message(), "utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("com.kk.mq.kafka.environment", "1");
//        for (int i = 0; i < 1000; i++) {
//            KafkaAdapter.getInstance().sendMessage("page_visits13", "world!--" + i);
//            Thread.sleep(1000);
//        }
        KafkaAdapter.getInstance().consume("group-7", "page_visits13", new KafkaConsume() {
            @Override
            public void run(String message) {
                System.out.println(message);
            }
        });
    }
}
