package kafkaThriftSerierlize.serializer;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Map;

public class ThriftSerializer implements Serializer<Person> {


	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public byte[] serialize(String topic, Person person) {
		try {
			TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			return ser.serialize(person);
		} catch (TException ex) {
			throw new SerializationException(
					"Can't serialize data='" + person + "' for topic='" + topic + "'", ex);
		}
	}

	@Override
	public void close() {

	}
}
