package com.bunnu.messaging;

import java.io.Serializable;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class MessageDto implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String name = "alpha";
	private final Long num = 1000L;
	private final int uid = 198;
	private final float n = 234.0f;

}
