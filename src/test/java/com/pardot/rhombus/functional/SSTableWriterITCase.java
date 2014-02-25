package com.pardot.rhombus.functional;

import com.datastax.driver.core.utils.UUIDs;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class SSTableWriterITCase extends RhombusFunctionalTest {

    private static Logger logger = LoggerFactory.getLogger(ObjectMapperUpdateITCase.class);


    @Test
    public void testInsertingAllNonNullValuesInSchema_simple() throws Exception {
        logger.debug("Starting testInsertingAllNonNullValuesInSchema");
        System.setProperty("cassandra.config", "cassandra-config/cassandra.yaml");

        //Build the connection manager
        ConnectionManager cm = getConnectionManager();

        //Build our keyspace definition object
        CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");
        definition.setName("sstablewriter_test");
        assertNotNull(definition);

        //Rebuild the keyspace and get the object mapper
        cm.buildKeyspace(definition, true);
        logger.debug("Built keyspace: {}", definition.getName());
        cm.setDefaultKeyspace(definition);
        ObjectMapper om = cm.getObjectMapper();
        om.setLogCql(true);

        //Insert our test data
        List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "SSTableWriterSimpleTestData.js");
        for (Map<String, Object> map : values) {
            map.put("id", UUIDs.timeBased());
        }
        Map<String, List<Map<String, Object>>> insert = new HashMap<String, List<Map<String, Object>>>();
        insert.put("simple", values);
        om.setSSTableOutputPath("sstablewriter_test/simple");
        om.insertIntoSSTable(insert);
        om.completeSSTableWrites();
    }
}
