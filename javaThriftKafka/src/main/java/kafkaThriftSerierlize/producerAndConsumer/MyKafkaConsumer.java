package kafkaThriftSerierlize.producerAndConsumer;

import kafkaThriftSerierlize.serializer.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void consumer() {//自动提交
        //1.创建消费者配置信息
        Properties properties = new Properties();
        //链接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.134.128:9092");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //自动提交的延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //key,value的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"kafkaThriftSerierlize.serializer.ThriftDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group02");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//重置消费者offset的方法（达到重复消费的目的），设置该属性也只在两种情况下生效：1.上面设置的消费组还未消费(可以更改组名来消费)2.该offset已经过期

        //创建生产者
        KafkaConsumer<String, Person> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("kafka-thrift")); //Arrays.asList()
        int i = 1;
        while (true) {
            //获取数据
            ConsumerRecords<String, Person> consumerRecords = consumer.poll(100);

            //解析并打印consumerRecords
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println("当前消费了第"+i+"条数据："+consumerRecord.key() + "----" + consumerRecord.value());
            }
            i++;
        }

        //consumer无需close()
    }
}
