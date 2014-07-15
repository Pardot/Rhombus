package com.pardot.rhombus;

/**
 * User: Michael Frank
 * Date: 8/19/13
 */
public class RhombusException extends Exception {

	public RhombusException(Exception e) {
		super(e);
	}

	public RhombusException(String message) {
		super(message);
	}
}
