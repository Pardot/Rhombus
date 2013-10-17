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

}

