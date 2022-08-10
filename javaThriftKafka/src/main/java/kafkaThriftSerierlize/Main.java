package kafkaThriftSerierlize;


import kafkaThriftSerierlize.producerAndConsumer.MyKafkaConsumer;
import kafkaThriftSerierlize.producerAndConsumer.MyKafkaProducer;

public class Main {
    public static void main(String[] args) {
//        MyKafkaProducer.producerData();
        MyKafkaConsumer.consumer();
    }
}
