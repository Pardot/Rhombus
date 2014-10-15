package com.pardot.rhombus;

import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.util.faker.FakeR;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class FakeRTest {
	@Test
	public void testGetMasterIteratorNoRange() throws Exception
	{
		Long wideRowsPerIndex = 2L;
		Long totalObjectsPerWideRange = 100L;
		Long totalObjectsPerShard = 10L;

		CKeyspaceDefinition ckdef = CKeyspaceDefinition.fromJsonFile("ShardedKeyspace.js");
		Map<String, CDefinition> cdefs = ckdef.getDefinitions();
		Integer totalIndexCount = 0;

		for (String cdefKey : cdefs.keySet()) {
			totalIndexCount += cdefs.get(cdefKey).getIndexes().size();
		}

		FakeR faker1 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		Iterator<Map<String, Object>> iterator = faker1.getMasterIterator(CObjectOrdering.ASCENDING, null, null);

		Map<String, Map<String, Object>> materializedObjects1 = Maps.newHashMap();
		while (iterator.hasNext()) {
			Map<String, Object> next = iterator.next();
			assertTrue(next.containsKey("id"));
			assertFalse("Materialized objects shouldn't have this id yet", materializedObjects1.containsKey(next.get("id").toString()));
			materializedObjects1.put(next.get("id").toString(), next);
		}
		assertFalse(materializedObjects1.isEmpty());
		assertEquals(totalObjectsPerWideRange * totalIndexCount, materializedObjects1.size());
		System.out.printf("Wide rows per index:%d, indexes per definition:1, objects per wide range:%d, objects per shard:%d, total object count:%d",
				wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard, materializedObjects1.size());

		Thread.sleep(500);

		// Let's do it again and see if we get the same results
		FakeR faker2 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		iterator = faker2.getMasterIterator(CObjectOrdering.ASCENDING, null, null);

		Map<String, Map<String, Object>> materializedObjects2 = Maps.newHashMap();
		while (iterator.hasNext()) {
			Map<String, Object> next = iterator.next();
			assertTrue(next.containsKey("id"));
			assertFalse("Materialized objects shouldn't have this id yet", materializedObjects2.containsKey(next.get("id").toString()));
			materializedObjects2.put(next.get("id").toString(), next);
		}
		assertFalse(materializedObjects2.isEmpty());
		assertEquals(totalObjectsPerWideRange * totalIndexCount, materializedObjects2.size());

		// Verify we got the same results both times
		assertEquals(materializedObjects1, materializedObjects2);
	}
}
