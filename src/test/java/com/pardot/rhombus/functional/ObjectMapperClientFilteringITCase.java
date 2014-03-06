package com.pardot.rhombus.functional;


import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class ObjectMapperClientFilteringITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperClientFilteringITCase.class);

	@Test
	public void testClientFilter() throws Exception {
		logger.debug("Starting testClientFilter");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);
		definition.getDefinitions().get("testtype");

		//Build the keyspace, get the object mapper, and truncate data
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper(definition.getName());
		om.setExecuteAsync(false);

		//Insert one object which is filtered and one which is not
		Map<String, Object> testObject = Maps.newHashMap();
		testObject.put("foreignid", 123l);
		testObject.put("filtered", 0);
		testObject.put("data1", "notfiltered");
		UUID notFilteredKey = (UUID)om.insert("testtype", testObject);

		testObject = Maps.newHashMap();
		testObject.put("foreignid", 123l);
		testObject.put("filtered", 1);
		testObject.put("data1", "filtered");
		UUID filteredKey = (UUID)om.insert("testtype", testObject);

		Thread.sleep(1000l);

		// Make sure we get both back when we query using a standard index
		Criteria foreignIdCriteria = new Criteria();
		foreignIdCriteria.setOrdering(CObjectOrdering.DESCENDING);
		foreignIdCriteria.setLimit(50l);
		foreignIdCriteria.setAllowFiltering(true);
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("foreignid", 123l);
		foreignIdCriteria.setIndexKeys(indexKeys);
		List<Map<String, Object>> dbObjects = om.list("testtype", foreignIdCriteria);
		assertEquals(2, dbObjects.size());

		// Make sure we only get one back when searching for not filtered
		Criteria foreignIdAndNotFilteredCriteria = new Criteria();
		foreignIdAndNotFilteredCriteria.setOrdering(CObjectOrdering.DESCENDING);
		foreignIdAndNotFilteredCriteria.setLimit(50l);
		foreignIdAndNotFilteredCriteria.setAllowFiltering(true);
		indexKeys = Maps.newTreeMap();
		indexKeys.put("foreignid", 123l);
		indexKeys.put("filtered", 0);
		foreignIdAndNotFilteredCriteria.setIndexKeys(indexKeys);
		dbObjects = om.list("testtype", foreignIdAndNotFilteredCriteria);
		assertEquals(1, dbObjects.size());
		assertEquals(notFilteredKey, dbObjects.get(0).get("id"));

		// Make sure we only get one back when searching for filtered
		Criteria foreignIdAndFilteredCriteria = new Criteria();
		foreignIdAndFilteredCriteria.setOrdering(CObjectOrdering.DESCENDING);
		foreignIdAndFilteredCriteria.setLimit(50l);
		foreignIdAndFilteredCriteria.setAllowFiltering(true);
		indexKeys = Maps.newTreeMap();
		indexKeys.put("foreignid", 123l);
		indexKeys.put("filtered", 1);
		foreignIdAndFilteredCriteria.setIndexKeys(indexKeys);
		dbObjects = om.list("testtype", foreignIdAndFilteredCriteria);
		assertEquals(1, dbObjects.size());
		assertEquals(filteredKey, dbObjects.get(0).get("id"));

		//Teardown connections
		cm.teardown();
	}

}
