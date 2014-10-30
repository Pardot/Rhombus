package com.pardot.rhombus.functional;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.TableScanner;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectTokenVisitor;
import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

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
		TableScanner scanner = new TableScanner(om, objectType, 1, visitorFactory, null);
		scanner.scan();

		long totalCount = 0;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			totalCount += visitor.getObjectCount();
		}

		assertEquals(20l, totalCount);
		cm.teardown();
	}

	@Test
	public void testFourPartitions() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";
		String savepointDirectoryName = "savepoint-test-dir";
		File savepointDirectory = new File(savepointDirectoryName);
		if (savepointDirectory.exists()) {
			FileUtils.deleteRecursive(savepointDirectory);
		}
		Integer numPartitions = 4;

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
		Long valueCount = 83L;
		List<Map<String, Object>> values = this.getNValues(valueCount, index1Value, index2Value);
		for(Map<String, Object> insertValue : values) {
			om.insert(objectType, insertValue);
		}

		VisitorFactoryTester visitorFactory = new VisitorFactoryTester();
		TableScanner scanner = new TableScanner(om, objectType, numPartitions, visitorFactory, savepointDirectoryName);
		List<Map.Entry<Long, Long>> ranges = scanner.makeRanges();
		scanner.scan();

		Long totalCount = 0L;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			totalCount += visitor.getObjectCount();
		}

		assertEquals(valueCount, totalCount);

		this.verifySavepoints(numPartitions, savepointDirectoryName, objectType, om, ranges);

		cm.teardown();
	}

	@Test
	public void testStartingFromSavepoint() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";
		String savepointDirectoryName = "savepoint-test-dir";
		File savepointDirectory = new File(savepointDirectoryName);
		if (savepointDirectory.exists()) {
			FileUtils.deleteRecursive(savepointDirectory);
		}
		Integer numPartitions = 2;

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
		Long valueCount = 83L;
		List<Map<String, Object>> values = this.getNValues(valueCount, index1Value, index2Value);
		for(Map<String, Object> insertValue : values) {
			om.insert(objectType, insertValue);
		}

		// Calculate halfway through each range
		UUID[] rangeHalfwayPoints = new UUID[numPartitions];
		BigInteger fullRange = BigInteger.valueOf(TableScanner.maxToken).subtract(BigInteger.valueOf(TableScanner.minToken));
		BigInteger rangeLength = fullRange.divide(BigInteger.valueOf(numPartitions));
		BigInteger rangeStart = BigInteger.valueOf(TableScanner.minToken);
		for(int i = 0 ; i < numPartitions - 1 ; i++) {
			BigInteger rangeEnd = rangeStart.add(rangeLength).subtract(BigInteger.ONE);
			Long halfwayToken = rangeLength.divide(BigInteger.valueOf(2)).add(rangeStart).longValue();
			rangeHalfwayPoints[i] = (UUID)om.scanTableWithStartToken(objectType, halfwayToken, TableScanner.maxToken, 1L).get(0).get("id");

			rangeStart = rangeEnd.add(BigInteger.ONE);
		}
		Long halfwayToken = rangeLength.divide(BigInteger.valueOf(2)).add(rangeStart).longValue();
		rangeHalfwayPoints[numPartitions - 1] = (UUID)om.scanTableWithStartToken(objectType, halfwayToken, TableScanner.maxToken, 1L).get(0).get("id");

		// Write halfway points to the savepoint directory
		savepointDirectory = new File(savepointDirectoryName);
		savepointDirectory.mkdir();

		for (int i = 0; i < numPartitions; i++) {
			String filename = TableScanner.getSavepointFilename(i);
			PrintWriter writer = new PrintWriter(new FileOutputStream(savepointDirectory.getName() + "/" + filename, false));
			writer.write(rangeHalfwayPoints[i].toString() + "\n");
			writer.close();
		}

		// Create and run scanner
		VisitorFactoryTester visitorFactory = new VisitorFactoryTester();
		TableScanner scanner = new TableScanner(om, objectType, numPartitions, visitorFactory, savepointDirectoryName);
		List<Map.Entry<Long, Long>> ranges = scanner.makeRanges();
		scanner.scan();

		Long actualCount = 0L;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			actualCount += visitor.getObjectCount();
		}

		// The total number of visits we have will fluctuate depending on where the ids land among the token ranges, but we should be close to half
		assertTrue(Math.abs(actualCount - (valueCount/2)) < (valueCount/(numPartitions * 2)));

		this.verifySavepoints(numPartitions, savepointDirectoryName, objectType, om, ranges);

		cm.teardown();
	}

	private void verifySavepoints(Integer numPartitions, String savepointDirectoryName, String objectType, ObjectMapper om, List<Map.Entry<Long, Long>> ranges) throws Exception {
		// Open up the savepoint directory
		File savepointDirectory = new File(savepointDirectoryName);
		assertTrue(savepointDirectory.exists());
		assertTrue(savepointDirectory.isDirectory());

		File[] fileArray = savepointDirectory.listFiles();
		assertNotNull(fileArray);

		// Make sure we have all the savepoint files we expect
		Map<String, File> files = Maps.newHashMap();
		for (File savepoint : fileArray) {
			files.put(savepoint.getName(), savepoint);
		}

		boolean foundAtLeastOneLine = false;
		UUID highestUuid = null;
		for (int i = 0; i < numPartitions; i++) {
			String filename = TableScanner.getSavepointFilename(i);
			assertTrue(files.containsKey(filename));

			File file = files.get(filename);
			ReversedLinesFileReader reader = new ReversedLinesFileReader(file);
			String line = reader.readLine();
			// A null line is actually ok, that just means there were no results in that partition's token range this time around
			if (line != null) {
				foundAtLeastOneLine = true;
				UUID savedUuid = UUID.fromString(line);
				assertNotNull(savedUuid);
				List<Map<String, Object>> values = om.scanTableWithStartId(objectType, savedUuid.toString(), TableScanner.maxToken, 1L);

				// This means there is no next id from this uuid so our normal check doesn't work.
				// This also means this is the highest uuid in the total range (might not be in the last partition if the last partition didn't end up with any objects)
				if (values.size() == 0) {
					// Make sure this only happens once
					assertNull(highestUuid);
					highestUuid = savedUuid;
				} else {
					// Otherwise there is a next uuid from the saved point, so compare that to the next uuid from the end of the partition's range and make sure they match
					UUID nextSavedUuid = (UUID) values.get(0).get("id");
					UUID nextExpectedUuid = (UUID) om.scanTableWithStartToken(objectType, ranges.get(i).getValue(), TableScanner.maxToken, 1L).get(0).get("id");
					assertEquals(nextExpectedUuid, nextSavedUuid);
				}
			}
		}
		assertTrue(foundAtLeastOneLine);
	}

	@Test
	public void testBigScan() throws Exception {
		String objectType = "simple";

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 2000l;
		long batchSize = 200;
		insertNObjects(om, insertNum, batchSize);

		VisitorFactoryTester visitorFactory = new VisitorFactoryTester();
		TableScanner scanner = new TableScanner(om, objectType, 1, visitorFactory, null);
		scanner.scan();

		long totalCount = 0;
		for(VisitorTester visitor : visitorFactory.getInstances()) {
			totalCount += visitor.getObjectCount();
		}

		assertEquals(insertNum, totalCount);
		cm.teardown();
	}

	public void insertNObjects(ObjectMapper om, long number, long batchSize) throws RhombusException, CQLGenerationException {
		if(number < batchSize) {
			batchSize = number;
		}
		long numInserted = 0;
		while(numInserted < number) {
			long numLeft = number - numInserted;
			long numToInsert = batchSize;
			if(numLeft < batchSize) {
				numToInsert = numLeft;
			}
			List<Map<String, Object>> objects = getNValues(numToInsert, "value1", "value2");
			Map<String, List<Map<String, Object>>> batch = Maps.newHashMap();
			batch.put("simple", objects);
			om.insertBatchMixed(batch);
			numInserted += numToInsert;
		}
	}

	private List<Map<String, Object>> getNValues(long number, String index1Value, String index2Value) {
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
