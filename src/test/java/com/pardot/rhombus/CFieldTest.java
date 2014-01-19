package com.pardot.rhombus;

import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.helpers.TestHelpers;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class CFieldTest extends TestCase{

	public void testEquals() {
		CField field1 = new CField("Name", CField.CDataType.ASCII);
		CField field2 = new CField("Name", CField.CDataType.ASCII);
		assertTrue(field1.equals(field2));
	}

	public void testNotEqualsType() {
		CField field1 = new CField("Name", CField.CDataType.ASCII);
		CField field2 = new CField("Name", CField.CDataType.BIGINT);
		assertFalse(field1.equals(field2));
	}

	public void testNotEqualsName() {
		CField field1 = new CField("Name", CField.CDataType.ASCII);
		CField field2 = new CField("Name2", CField.CDataType.ASCII);
		assertFalse(field1.equals(field2));
	}
}

