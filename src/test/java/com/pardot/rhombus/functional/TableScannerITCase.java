package com.pardot.rhombus.functional;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.TableScanner;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectTokenVisitor;
import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TableScannerITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(TableScannerITCase.class);


	@Test
	public void testOnePartition() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		// Insert 20 values
		List<Map<String, Object>> values = this.getNValues(20, index1Value, index2Value);
		for(Map<String, Object> insertValue : values) {
			om.insert(objectType, insertValue);
		}

		VisitorFactoryTester visitorFactory = new VisitorFactoryTester();
		TableScanner scanner = new TableScanner(om, objectType, 1, visitorFactory);
		scanner.scan();

		long totalCount = 0;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			totalCount += visitor.getObjectCount();
		}

		assertEquals(20l, totalCount);
	}

	@Test
	public void testFourPartitions() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		// Insert 83 values
		List<Map<String, Object>> values = this.getNValues(83, index1Value, index2Value);
		for(Map<String, Object> insertValue : values) {
			om.insert(objectType, insertValue);
		}

		VisitorFactoryTester visitorFactory = new VisitorFactoryTester();
		TableScanner scanner = new TableScanner(om, objectType, 4, visitorFactory);
		scanner.scan();

		long totalCount = 0;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			totalCount += visitor.getObjectCount();
		}

		assertEquals(83l, totalCount);
	}

	private List<Map<String, Object>> getNValues(int number, String index1Value, String index2Value) {
		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
		for(int i = 0 ; i < number ; i++) {
			Map<String, Object> value = Maps.newHashMap();
			value.put("index_1", index1Value);
			value.put("index_2", index2Value);
			value.put("value", String.valueOf(i));
			values.add(value);
		}
		return values;
	}

	private class VisitorTester extends CObjectTokenVisitor {

		private long objectCount = 0;

		public VisitorTester(String name) {
			super(name);
		}

		@Override
		public void applyAction(Map<String, Object> object) {
			objectCount++;
		}

		@Override
		public boolean shouldInclude(Map<String, Object> object) {
			return true;
		}

		@Override
		public void setUp() {

		}

		@Override
		public void cleanUp() {

		}

		public long getObjectCount() {
			return objectCount;
		}
	}

	private class VisitorFactoryTester implements CObjectTokenVisitorFactory {

		private List<VisitorTester> instances;

		public VisitorFactoryTester() {
			instances = Lists.newArrayList();
		}

		@Override
		public CObjectTokenVisitor getInstance(String name) {
			VisitorTester tester = new VisitorTester(name);
			instances.add(tester);
			return tester;
		}

		public List<VisitorTester> getInstances() {
			return instances;
		}
	}
}
