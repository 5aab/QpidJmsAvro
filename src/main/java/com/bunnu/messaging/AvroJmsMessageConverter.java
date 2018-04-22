package com.bunnu.messaging;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.lang.SerializationException;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class AvroJmsMessageConverter implements MessageConverter {

	@Override
	public Object fromMessage(Message message) throws JMSException, MessageConversionException {
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs
				.toBinary(ReflectData.get().getSchema(MessageDto.class));
		MessageDto dto = deserialize(extractByteArrayFromMessage(message));
		System.out.println(dto);
		return recordInjection.invert(extractByteArrayFromMessage(message)).get();
	}

	@Override
	public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs
				.toBinary(ReflectData.get().getSchema(MessageDto.class));
		StreamMessage message = session.createStreamMessage();
		message.writeBytes(recordInjection.apply((GenericRecord) object));
		return message;
	}

	private static byte[] extractByteArrayFromMessage(Message message) throws JMSException {
		StreamMessage msg = (StreamMessage) message;
		int BUFFER_CAPACITY_BYTES = 100;
		ByteArrayOutputStream oStream = new ByteArrayOutputStream(BUFFER_CAPACITY_BYTES);
		byte[] buffer = new byte[BUFFER_CAPACITY_BYTES];
		int bufferCount = -1;
		while ((bufferCount = msg.readBytes(buffer)) >= 0) {
			oStream.write(buffer, 0, bufferCount);
			if (bufferCount < BUFFER_CAPACITY_BYTES) {
				break;
			}
		}
		return oStream.toByteArray();
	}

	public MessageDto deserialize(byte[] data) {
		// LOGGER.debug("data to deserialize='{}'",
		// DatatypeConverter.printHexBinary(data));
		try {
			// get the schema
			Schema schema = ReflectData.get().getSchema(MessageDto.class);

			Injection<GenericRecord, byte[]> genericRecordInjection = GenericAvroCodecs.toBinary(schema);
			GenericRecord genericRecord = genericRecordInjection.invert(data).get();
			MessageDto result = (MessageDto) SpecificData.get().deepCopy(schema, genericRecord);

			// LOGGER.debug("data='{}'", result);
			return result;
		} catch (Exception e) {
			throw new SerializationException(
					"Can't deserialize data [" + Arrays.toString(data) + "] from topic [" + "]", e);
		}
	}
}
