package kafkaThriftSerierlize.producerAndConsumer;

import kafkaThriftSerierlize.serializer.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaProducer {

    public static void  producerData() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.134.128:9092");
        properties.put("acks", "all");
        properties.put("delivery.timeout.ms", 30000);
        properties.put("batch.size", 1000);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","kafkaThriftSerierlize.serializer.ThriftSerializer");

        Producer<String, Person> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            Person person= new Person();
            person.setAge(1);
            person.setUsername("张三");
            producer.send(new ProducerRecord<>("kafka-thrift", null, person));
        }
        producer.close();
        System.out.println("生产者程序运行结束。");
    }
}
