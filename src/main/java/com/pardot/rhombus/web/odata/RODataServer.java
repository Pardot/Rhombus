package com.pardot.rhombus.web.odata;

import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.odata4j.core.Throwables;
import org.odata4j.cxf.producer.server.ODataCxfServer;
import org.odata4j.examples.ODataServerFactory;
import org.odata4j.jersey.producer.resources.ODataApplication;
import org.odata4j.jersey.producer.server.ODataJerseyServer;
import org.odata4j.producer.resources.DefaultODataApplication;
import org.odata4j.producer.resources.DefaultODataProducerProvider;
import org.odata4j.producer.resources.RootApplication;
import org.odata4j.producer.server.ODataServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static org.odata4j.examples.JaxRsImplementation.JERSEY;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/9/14
 */
public class RODataServer {
	private static Logger logger;

	public static void main(String[] args) {
		try {
			Logger rootLogger = LogManager.getLogManager().getLogger("");
			Handler[] handlers = rootLogger.getHandlers();
			for(int i = 0 ; i < handlers.length ; i++) {
				System.out.println(handlers[i].getClass());
			}
			Properties systemProperties = System.getProperties();
			Enumeration enuProp = systemProperties.propertyNames();
			while (enuProp.hasMoreElements()) {
				String propertyName = (String) enuProp.nextElement();
				String propertyValue = systemProperties.getProperty(propertyName);
				System.out.println(propertyName + ": " + propertyValue);
			}
			logger = LogManager.getLogManager().getLogger(RODataServer.class.getName());
			String cassConfigPath = args[0];
			String keyspaceConfigPath = args[1];
			DefaultODataProducerProvider.setInstance(createProducer(cassConfigPath, keyspaceConfigPath));
			hostODataServer("http://localhost:8888/pardot.svc/");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static RODataProducer createProducer(String cassConfigPath, String keyspaceConfigPath) throws Exception {
		CassandraConfiguration cassConfig = JsonUtil.objectFromJsonResource(CassandraConfiguration.class, CassandraConfiguration.class.getClassLoader(), cassConfigPath);
		CKeyspaceDefinition keyspaceDefinition = CKeyspaceDefinition.fromJsonFile(keyspaceConfigPath);

		//Add managed connection manager
		ConnectionManager cm = new ConnectionManager(cassConfig);
		cm.setLogCql(false);
		cm.setDefaultKeyspace(keyspaceDefinition);
		cm.buildCluster();

		//Pre set up our default keyspace so 2000 requests don't try to do it at the same time
		ObjectMapper om = cm.getObjectMapper(keyspaceDefinition);

		return new RODataProducer(om);
	}

	public static void hostODataServer(String baseUri) {
		ODataServer server = null;
		try {
			server = startODataServer(baseUri);
			//logger.log(Level.INFO, "Started odata server");
			System.out.println("Press any key to exit");


			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} catch (IOException e) {
			throw Throwables.propagate(e);
		} finally {
			if (server != null)
				server.stop();
		}
	}

	public static ODataServer startODataServer(String baseUri) {
		return createODataServer(baseUri).start();
	}

	public static ODataServer createODataServer(String baseUri) {
		return new RODataJerseyServer(baseUri, ODataApplication.class, RootApplication.class);
	}
}
