package com.pardot.rhombus.helpers;

import com.google.common.collect.Lists;
import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class TestHelpers {
	private static Logger logger = LoggerFactory.getLogger(TestHelpers.class);

	private static List<Map<String, Object>> testObjects;
	private static CriteriaHolder criteriaHolder;

	public static ConnectionManagerTester getTestConnectionManager() throws IOException {
		ConnectionManagerTester cm = new ConnectionManagerTester(TestHelpers.getTestCassandraConfiguration());
		cm.setLogCql(false);
		String nativeTransportPort = System.getProperty("cassandra.nativeTransportPort");
		try {
			Integer port = Integer.parseInt(nativeTransportPort);
			logger.info("Found a native transport port {}", port);
			cm.setNativeTransportPort(port);
		} catch (Exception e) {
			//Ignore
		}
		return cm;
	}

	public static String readFileToString(Class testclass, String filename){
		String ret = "";
		try{
			InputStream inputStream;
			inputStream = testclass.getClassLoader().getResourceAsStream(filename);
			ret = IOUtils.toString(inputStream);
			inputStream.close();
		}
		catch (Exception e){
            e.printStackTrace();
		}
		return ret;
	}

	public static CassandraConfiguration getTestCassandraConfiguration() throws IOException {
		return JsonUtil.objectFromJsonResource(CassandraConfiguration.class, CassandraConfiguration.class.getClassLoader(), "cassandra-functional.js");
	}

	public static Map<String, Object> getTestObject(int index) {
		if(testObjects == null) {
			try {
				testObjects = (List<Map<String, Object>>)JsonUtil.objectFromJsonResource(List.class, TestHelpers.class.getClassLoader(), "TestObjects.js");
			} catch (IOException e) {
				testObjects = Lists.newArrayList();
			}
		}
		return testObjects.get(index);
	}

	public static Criteria getTestCriteria(int index) {
		if(criteriaHolder == null) {
			try {
				criteriaHolder = JsonUtil.objectFromJsonResource(CriteriaHolder.class, TestHelpers.class.getClassLoader(), "TestCriteria.js");
			} catch (IOException e) {
				criteriaHolder = new CriteriaHolder();
			}
		}
		return criteriaHolder.getCriteria().get(index);
	}

	static class CriteriaHolder {
		private List<Criteria> criteria;

		List<Criteria> getCriteria() {
			return criteria;
		}

		void setCriteria(List<Criteria> criteria) {
			this.criteria = criteria;
		}
	}

//	public static Map<String,Object> convertStringsToRealTypes(CDefinition def, Map<String,Object> values){
//		Map<String,Object> ret = Maps.newTreeMap();
//		for(String key : values.keySet()){
//			String valueAsString = (String)values.get(key);
//			if((def.getFields().get(key).getType() == CField.CDataType.UUID) || (def.getFields().get(key).getType() == CField.CDataType.TIMEUUID)){
//				ret.put(key, UUID.fromString(valueAsString));
//			}
//			else if( (def.getFields().get(key).getType() == CField.CDataType.TIMESTAMP) ||
//				(def.getFields().get(key).getType() == CField.CDataType.INT)
//			){
//				ret.put(key, Long.parseLong(valueAsString));
//			}
//			else if(def.getFields().get(key).getType() == CField.CDataType.BOOLEAN){
//				ret.put(key, Boolean.parseBoolean(valueAsString));
//			}
//			else{
//				ret.put(key,valueAsString);
//			}
//
//		}
//		return ret;
//	}
}
