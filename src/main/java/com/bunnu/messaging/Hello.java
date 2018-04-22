package com.bunnu.messaging;

import java.io.FileInputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class Hello {

	public Hello() {
	}

	public static void main2(String[] args) {
		Hello hello = new Hello();
		hello.runTest();
	}

	private void runTest() {
		Properties properties = new Properties();
		try {
			properties.load(
					new FileInputStream("C:/Users/ISH/workspace/SpringBootSource/src/main/resources/hello.properties"));

			Context context = new InitialContext(properties);

			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
			Connection connection = connectionFactory.createConnection();
			connection.start();

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = (Destination) context.lookup("topicExchange");

			MessageProducer messageProducer = session.createProducer(destination);
			MessageConsumer messageConsumer = session.createConsumer(destination);

			TextMessage message = session.createTextMessage("Hello world!");
			messageProducer.send(message);

			message = (TextMessage) messageConsumer.receive();
			System.out.println(message.getText());

			connection.close();
			context.close();
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}
}
