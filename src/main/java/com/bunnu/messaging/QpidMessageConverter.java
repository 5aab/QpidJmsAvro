package com.bunnu.messaging;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

public class QpidMessageConverter implements MessageConverter {

	private MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();

	private final AvroJmsMessageConverter converterAvro = new AvroJmsMessageConverter();

	public QpidMessageConverter() {
		converter = new MappingJackson2MessageConverter();
		// converter.setTargetType(MessageType.TEXT);
		Map<String, Class<?>> typeids = new HashMap<>();
		typeids.put("POJO", MessageDto.class);
		converter.setTypeIdMappings(typeids);
		converter.setTypeIdPropertyName("POJO");
	}

	// @Override
	@Override
	public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {
		return converterAvro.toMessage(object, session);
	}

	// @Override
	@Override
	public Object fromMessage(Message message) throws JMSException, MessageConversionException {
		return converterAvro.fromMessage(message);
	}

}
