package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Sets;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.faker.FakeCIndex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * User: Rob Righter
 * Date: 3/15/14
 */
public class FakeCIndexTest {
	@Test
	public void getMasterIteratorTest() throws Exception {

		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def = CDefinition.fromJsonString(json);
		Long millistamp = System.currentTimeMillis();
		UUID startingUUID = UUIDs.startOf(millistamp);

		FakeCIndex subject = new FakeCIndex(
				def.getIndexes().get("instance:type"),
				def,
				startingUUID,
				100L,
				50L,
				10L );
		Iterator<Map<String,Object>> masterIt = subject.getMasterIterator(CObjectOrdering.DESCENDING);
		Set<String> idSet = Sets.newHashSet();

		long counter = 0;
		while(masterIt.hasNext()){
			counter++;
			Map<String,Object> item = masterIt.next();
			assertFalse("Id is not unique", idSet.contains(item.get("id").toString()));
			idSet.add(item.get("id").toString());
			System.out.println(item);
		}

		assertEquals(50L,counter);


	}
}
