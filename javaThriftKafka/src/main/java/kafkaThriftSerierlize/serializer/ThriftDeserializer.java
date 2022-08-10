package kafkaThriftSerierlize.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Arrays;
import java.util.Map;


public class ThriftDeserializer implements Deserializer<Person> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public Person deserialize(String topic, byte[] data) {
    try {
      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      Person person = new Person();
      deserializer.deserialize(person, data);

      return person;
    } catch (Exception ex) {
      throw new SerializationException(
              "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
    }
  }

  @Override
  public void close() {

  }
}
