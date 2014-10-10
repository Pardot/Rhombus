package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.util.faker.FakeR;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FakeRTest {
	@Test
	public void testGetMasterIteratorNoRange() throws Exception
	{
		Long wideRowsPerIndex = 2L;
		Long totalObjectsPerWideRange = 3L;
		Long totalObjectsPerShard = 7L;

		CKeyspaceDefinition ckdef = CKeyspaceDefinition.fromJsonFile("ShardedKeyspace.js");
		Map<String, CDefinition> cdefs = ckdef.getDefinitions();
		Integer totalIndexCount = 0;

		for (String cdefKey : cdefs.keySet()) {
			totalIndexCount += cdefs.get(cdefKey).getIndexes().size();
		}

		FakeR faker1 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		Iterator<Map<String, Object>> iterator = faker1.getMasterIterator(CObjectOrdering.ASCENDING, null, null);

		List<Map<String, Object>> materializedObjects1 = Lists.newArrayList();
		while (iterator.hasNext()) {
			materializedObjects1.add(iterator.next());
		}
		assertFalse(materializedObjects1.isEmpty());
		assertEquals((wideRowsPerIndex*totalObjectsPerWideRange)*totalIndexCount, materializedObjects1.size());
		System.out.printf("Wide rows per index:%d, indexes per definition:1, objects per wide range:%d, objects per shard:%d, total object count:%d",
				wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard, materializedObjects1.size());

		Thread.sleep(500);

		// Let's do it again and see if we get the same results
		FakeR faker2 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		iterator = faker2.getMasterIterator(CObjectOrdering.ASCENDING, null, null);

		List<Map<String, Object>> materializedObjects2 = Lists.newArrayList();
		while (iterator.hasNext()) {
			materializedObjects2.add(iterator.next());
		}
		assertFalse(materializedObjects2.isEmpty());
		assertEquals((wideRowsPerIndex*totalObjectsPerWideRange)*totalIndexCount, materializedObjects2.size());

		// Verify we got the same results both times
		assertEquals(materializedObjects1, materializedObjects2);
	}

	@Test
	@Ignore
	public void testGetMasterIteratorRange() throws Exception
	{
		Long wideRowsPerIndex = 2L;
		Long totalObjectsPerWideRange = 3L;
		Long totalObjectsPerShard = 7L;

		CKeyspaceDefinition ckdef = CKeyspaceDefinition.fromJsonFile("ShardedKeyspace.js");
		Map<String, CDefinition> cdefs = ckdef.getDefinitions();
		Integer totalIndexCount = 0;

		for (String cdefKey : cdefs.keySet()) {
			totalIndexCount += cdefs.get(cdefKey).getIndexes().size();
		}
		// Fake the data without a counter range to start
		FakeR faker1 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		Iterator<Map<String, Object>> iterator = faker1.getMasterIterator(CObjectOrdering.ASCENDING, null, null);

		List<Map<String, Object>> materializedObjects1 = Lists.newArrayList();
		while (iterator.hasNext()) {
			materializedObjects1.add(iterator.next());
		}
		assertFalse(materializedObjects1.isEmpty());
		assertEquals((wideRowsPerIndex*totalObjectsPerWideRange)*totalIndexCount, materializedObjects1.size());
		// Now define an id range from our materialized objects
		UUID startId = (UUID)materializedObjects1.get(1).get("id");
		UUID endId = (UUID)materializedObjects1.get((int)(wideRowsPerIndex*totalObjectsPerWideRange)-1).get("id");
//		Range<UUID> idRange = Range.closed(startId, endId);
//		Range<Long> idRange = Range.closed(1L, wideRowsPerIndex*totalObjectsPerWideRange-1);

		System.out.printf("Wide rows per index:%d, indexes per definition:1, objects per wide range:%d, objects per shard:%d, total object count:%d",
				wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard, materializedObjects1.size());

		Thread.sleep(500);

		// Let's do it again and see if we get the same results
		FakeR faker2 = new FakeR(ckdef, wideRowsPerIndex, totalObjectsPerWideRange, totalObjectsPerShard);
		iterator = faker2.getMasterIterator(CObjectOrdering.ASCENDING, startId, endId);

		List<Map<String, Object>> materializedObjects2 = Lists.newArrayList();
		while (iterator.hasNext()) {
			materializedObjects2.add(iterator.next());
		}
		assertFalse(materializedObjects2.isEmpty());

		// Verify we got the same results both times
		assertEquals(materializedObjects1, materializedObjects2);
	}

	@Test
	public void testUUIDGeneration() throws InterruptedException {
		Long time = System.currentTimeMillis() / 1000;

		UUID first = UUIDs.startOf(time);
		Thread.sleep(1000);
		UUID second = UUIDs.startOf(time);

		assertNotEquals(first, second);
	}
}
