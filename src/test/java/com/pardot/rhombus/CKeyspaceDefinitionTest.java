package com.pardot.rhombus;

import com.datastax.driver.core.ConsistencyLevel;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class CKeyspaceDefinitionTest extends TestCase{

    public void testFields() throws IOException {
        CKeyspaceDefinition def = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		assertEquals(ConsistencyLevel.QUORUM, def.getConsistencyLevel());
    }

	public void testEquals() throws IOException {
		CKeyspaceDefinition def1 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		CKeyspaceDefinition def2 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		assertTrue(def1.equals(def2));
	}

	public void testNotEquals() throws IOException {
		CKeyspaceDefinition def1 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		CKeyspaceDefinition def2 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		def2.setName("Other name");
		assertFalse(def1.equals(def2));
	}

	public void testNotEqualsReplicationFactors() throws IOException {
		CKeyspaceDefinition def1 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		CKeyspaceDefinition def2 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		def2.getReplicationFactors().put("Other factor", 33);
		assertFalse(def1.equals(def2));
	}

	public void testNotEqualsConsistencyLevel() throws IOException {
		CKeyspaceDefinition def1 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		CKeyspaceDefinition def2 = CKeyspaceDefinition.fromJsonFile("QuorumKeyspace.js");
		def2.setConsistencyLevel(ConsistencyLevel.ALL);
		assertFalse(def1.equals(def2));
	}
}

