package com.pardot.rhombus.cobject.migrations;

/**
 * User: Rob Righter
 * Date: 10/8/13
 */
public class CObjectMigrationException extends Exception {

	public CObjectMigrationException(String message){
		super(message);
	}

	public CObjectMigrationException(Exception cause) {
		super(cause);
	}

}
