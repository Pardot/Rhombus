package com.pardot.rhombus;

import com.pardot.rhombus.cobject.statement.CQLStatement;
import junit.framework.TestCase;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class CQLStatementTest extends TestCase {

	private static final String query = "insert into object (field1, field2) values (?, ?);";
	private static final String query2 = "insert into object (field1, field2, field3) values (?, ?, ?);";
	private static final String[] values = {"value1", "value2"};
	private static final String[] values2 = {"value1", "value2", "value3"};
	private static final String objectName = "object";

	public void testMake() {
		CQLStatement statement = CQLStatement.make(query, objectName);
		assertEquals(query, statement.getQuery());
		assertEquals(objectName, statement.getObjectName());
		assertNull(statement.getValues());
	}

	public void testMakeWithValues() {
		CQLStatement statement = CQLStatement.make(query, objectName, values);
		assertEquals(query, statement.getQuery());
		assertEquals(objectName, statement.getObjectName());
		assertEquals(values, statement.getValues());
	}

	public void testSetGetQuery() {
		CQLStatement statement = CQLStatement.make(null, null);
		statement.setQuery(query);
		assertEquals(statement.getQuery(), query);
	}

	public void testSetGetValues() {
		CQLStatement statement = CQLStatement.make(null, null);
		statement.setValues(values);
		assertEquals(statement.getValues(), values);
	}

	public void testIsPreparableFalse() {
		CQLStatement statement = CQLStatement.make(query, null);
		assertFalse(statement.isPreparable());
	}

	public void testIsPreparableTrue() {
		CQLStatement statement = CQLStatement.make(query, objectName, values);
		assertTrue(statement.isPreparable());
	}

	public void testCompareToEquals() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		CQLStatement statement2 = CQLStatement.make(query, objectName);
		assertEquals(0, statement1.compareTo(statement2));
	}

	public void testCompareToLess() {
		CQLStatement statement1 = CQLStatement.make(query, objectName, values);
		CQLStatement statement2 = CQLStatement.make(query, objectName, values2);
		assertEquals(-1, statement1.compareTo(statement2));
	}

	public void testCompareToGreater() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		CQLStatement statement2 = CQLStatement.make(query2, objectName);
		assertEquals(1, statement1.compareTo(statement2));
	}

	public void testEqualsDifferentClass() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		Integer notStatement = new Integer(1);
		assertFalse(statement1.equals(notStatement));
	}

	public void testEqualsDifferentQuery() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		CQLStatement statement2 = CQLStatement.make(query2, objectName);
		assertFalse(statement1.equals(statement2));
	}

	public void testEqualsNullValue() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		CQLStatement statement2 = CQLStatement.make(query, objectName, values);
		assertFalse(statement1.equals(statement2));
	}

	public void testEqualsDifferentValues() {
		CQLStatement statement1 = CQLStatement.make(query, objectName, values);
		CQLStatement statement2 = CQLStatement.make(query, objectName, values2);
		assertFalse(statement1.equals(statement2));
	}

	public void testEquals() {
		CQLStatement statement1 = CQLStatement.make(query, objectName, values);
		CQLStatement statement2 = CQLStatement.make(query, objectName, values);
		assertTrue(statement1.equals(statement2));
	}

	public void testToString() {
		CQLStatement statement1 = CQLStatement.make(query, objectName);
		String expected = "Query: insert into object (field1, field2) values (?, ?);\n" +
				"Values: null\n" +
				"Preparable: false";
		assertEquals(expected, statement1.toString());
	}

	public void testToStringValues() {
		CQLStatement statement1 = CQLStatement.make(query, objectName, values);
		String expected = "Query: insert into object (field1, field2) values (?, ?);\n" +
				"Values: [\n" +
				"    value1 (class java.lang.String) ,\n" +
				"    value2 (class java.lang.String) \n" +
				"\n" +
				"]\n" +
				"Preparable: true";
		assertEquals(expected, statement1.toString());
	}

	public void testGetSetIsCacheable() {
		CQLStatement statement = CQLStatement.make(query, objectName);
		statement.setCacheable(true);
		assertTrue(statement.isCacheable());
	}

	public void testGetSetObjectName() {
		CQLStatement statement = CQLStatement.make(query, objectName);
		statement.setObjectName(objectName);
		assertEquals(objectName, objectName);
	}
}

