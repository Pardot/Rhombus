package com.pardot.rhombus;

/**
 * User: Michael Frank
 * Date: 8/19/13
 */
public class RhombusTimeoutException extends RuntimeException {

	public RhombusTimeoutException(Exception e) {
		super(e);
	}

	public RhombusTimeoutException(String message) {
		super(message);
	}
}
