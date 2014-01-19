package com.pardot.rhombus;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.migrations.CKeyspaceDefinitionMigrator;
import com.pardot.rhombus.cobject.migrations.CObjectMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 4/17/13
 */
public class ConnectionManager {

	private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);


	private List<String> contactPoints;
	private final String localDatacenter;
	private Map<String, ObjectMapper> objectMappers = Maps.newHashMap();
	private CKeyspaceDefinition defaultKeyspace;
	private Cluster cluster;
	private boolean logCql = false;
	private Integer nativeTransportPort = null;
	private Long batchTimeout = 10000L;
	private Integer individualNodeConnectionTimeout = 2000;
	private Integer driverReadTimeoutMillis = 2000;
	private Integer consistencyHorizon = null;
	private LoadBalancingPolicy loadBalancingPolicy = null;
	private Integer maxConnectionPerHostLocal = null;
	private Integer maxConnectionPerHostRemote = null;
	private Integer maxSimultaneousRequestsPerConnectionTreshold = null;

	private String rhombusKeyspaceName = "rhombus_data";
	private Session rhombusSession = null;
	private ObjectMapper rhombusObjectMapper = null;

	public ConnectionManager(CassandraConfiguration configuration) {
		this.contactPoints = configuration.getContactPoints();
		this.localDatacenter = configuration.getLocalDatacenter();
		this.consistencyHorizon = configuration.getConsistencyHorizion();
		this.maxConnectionPerHostLocal = configuration.getMaxConnectionPerHostLocal() == null ? 16 : configuration.getMaxConnectionPerHostLocal();
		this.maxConnectionPerHostRemote = configuration.getMaxConnectionPerHostRemote() == null ? 4 : configuration.getMaxConnectionPerHostRemote();
		this.maxSimultaneousRequestsPerConnectionTreshold = configuration.getMaxSimultaneousRequestsPerConnectionTreshold() == null ? 128 : configuration.getMaxSimultaneousRequestsPerConnectionTreshold();

		if(configuration.getIndividualNodeConnectionTimeout() != null) {
			this.individualNodeConnectionTimeout = configuration.getIndividualNodeConnectionTimeout();
		}
		if(configuration.getDriverReadTimeoutMillis() != null) {
			this.driverReadTimeoutMillis = configuration.getDriverReadTimeoutMillis();
		}
		if(configuration.getBatchTimeout() != null) {
			this.batchTimeout = configuration.getBatchTimeout();
		}
		if(configuration.getRhombusKeyspaceName() != null) {
			this.rhombusKeyspaceName = configuration.getRhombusKeyspaceName();
		}
	}

	/**
	 * Build the cluster based on the CassandraConfiguration passed in the constructor
	 */
    public Cluster buildCluster(){
        return buildCluster(false);
    }

	public Cluster buildCluster(boolean withoutJMXReporting) {
		Cluster.Builder builder = Cluster.builder();
		for(String contactPoint : contactPoints) {
			builder.addContactPoint(contactPoint);
		}
		if(localDatacenter != null) {
			logger.info("Creating with DCAwareRoundRobinPolicy: {}", localDatacenter);
			if(loadBalancingPolicy == null) {
				loadBalancingPolicy = new DCAwareRoundRobinPolicy(localDatacenter);
			}
			builder.withLoadBalancingPolicy(new TokenAwarePolicy(loadBalancingPolicy));
		}
		if(this.nativeTransportPort != null) {
			logger.debug("Setting native transport port to {}", this.nativeTransportPort);
			builder.withPort(this.nativeTransportPort);
		}
		PoolingOptions poolingOptions = new PoolingOptions();
		if(maxConnectionPerHostLocal != null){
			poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,maxConnectionPerHostLocal);
		}
		if(maxConnectionPerHostRemote != null){
			poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,maxConnectionPerHostRemote);
		}
		if(maxSimultaneousRequestsPerConnectionTreshold != null){
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL,maxSimultaneousRequestsPerConnectionTreshold);
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE,maxSimultaneousRequestsPerConnectionTreshold);

		}
		builder.withPoolingOptions(poolingOptions);
		SocketOptions socketOptions = new SocketOptions();
		socketOptions.setConnectTimeoutMillis(individualNodeConnectionTimeout);
		socketOptions.setReadTimeoutMillis(driverReadTimeoutMillis);
		builder.withSocketOptions(socketOptions);
        if(withoutJMXReporting){
            cluster = builder.withoutJMXReporting().build();
        }
        else{
            cluster = builder.build();
        }
		cluster.init();

		return cluster;
	}

	/**
	 * Lazy getter for cached Rhombus session.  Will create the keyspace if it does not exist.
	 * @param definition Keyspace definition to use as a template if we are creating the Rhombus keyspace
	 * @return Cached Rhombus session
	 */
	protected Session getRhombusSession(CKeyspaceDefinition definition) {
		if(this.rhombusSession == null) {
			logger.debug("Creating new Rhombus session");
			try {
				this.rhombusSession = cluster.connect(rhombusKeyspaceName);
			} catch(InvalidQueryException e) {
				logger.debug("Unable to connect to Rhombus keyspace: {}, attempting to create it", this.rhombusKeyspaceName);
				createKeyspaceIfNotExists(rhombusKeyspaceName, definition, true);
				this.rhombusSession = cluster.connect(rhombusKeyspaceName);
			}
		}
		return this.rhombusSession;
	}

	/**
	 * Lazy getter for cached Rhombus object mapper.  This will also lazy get the Rhombus session
	 * Since this happens once per instantiation of Rhombus, it is not a big deal to make sure
	 * that our keyspace definition table exists.
	 * @param definition Keyspace definition to use as a template if we are creating the Rhombus keyspace
	 * @return Cached RhombusObjectMapper
	 */
	protected ObjectMapper getRhombusObjectMapper(CKeyspaceDefinition definition) {
		if(this.rhombusObjectMapper == null) {
			logger.debug("Creating new Rhombus object mapper");
			if(definition == null) {
				definition = defaultKeyspace;
			}
			CKeyspaceDefinition rhombusKeyspaceDefinition = new CKeyspaceDefinition();
			rhombusKeyspaceDefinition.setName(rhombusKeyspaceName);
			this.rhombusObjectMapper = new ObjectMapper(getRhombusSession(definition), rhombusKeyspaceDefinition, consistencyHorizon, batchTimeout);
			this.rhombusObjectMapper.createKeyspaceDefinitionTableIfNotExists();
		}
		return this.rhombusObjectMapper;
	}

	/**
	 *
	 * @param keyspaceName Name of keyspace to get/create
	 * @param keyspace The keyspace to use as a template for replication information
	 * @return true if keyspace existed previously, false if it was created
	 */
	private boolean createKeyspaceIfNotExists(String keyspaceName, CKeyspaceDefinition keyspace, boolean alterIfExists) {
		Preconditions.checkNotNull(keyspace, "A template keyspace must be supplied for replication information");
		Session session = cluster.connect();
		//First try to create the new keyspace
		StringBuilder sb = new StringBuilder();
		sb.append(keyspaceName);
		sb.append(" WITH replication = { 'class' : '");
		sb.append(keyspace.getReplicationClass());
		sb.append("'");
		for(String key : keyspace.getReplicationFactors().keySet()) {
			sb.append(", '");
			sb.append(key);
			sb.append("' : ");
			sb.append(keyspace.getReplicationFactors().get(key));
		}
		sb.append("};");
		try {
			String cql = "CREATE KEYSPACE " + sb.toString();
			session.execute(cql);
			session.shutdown();
			return false;
		} catch(AlreadyExistsException e) {
			logger.debug("Keyspace {} already exists", keyspaceName);
			// If the keyspace already existed, alter it to match the definition
			if(alterIfExists) {
				try {
					session.execute("ALTER KEYSPACE " + sb.toString());
				} catch(Exception e2) {
					logger.error("Unable to alter keyspace {}", keyspaceName, e2);
				}
			}
			session.shutdown();
			return true;
		}
	}

	/**
	 * Get the default object mapper
	 * @return The default object mapper
	 */
	public ObjectMapper getObjectMapper() throws Exception {
		return getObjectMapper(defaultKeyspace);
	}

	/**
	 * Gets an object mapper for the specified keyspace definition
	 * This method prefers keyspace definitions stored in the Rhombus data store in Cassandra
	 * This method will throw a warning if the definition passed in does not match the definition in Cassandra,
	 * as this likely indicates that a migration needs to be run.
	 * @param keyspaceDefinition The definition to get an object mapper for
	 * @return created object mapper
	 */
	public ObjectMapper getObjectMapper(CKeyspaceDefinition keyspaceDefinition) throws Exception {
		String keyspaceName = keyspaceDefinition.getName();
		ObjectMapper objectMapper = objectMappers.get(keyspaceName);
		if(objectMapper == null) {
			CKeyspaceDefinition rhombusKeyspaceDefinition = null;
			rhombusKeyspaceDefinition = hydrateLatestKeyspaceDefinitionFromCassandra(keyspaceDefinition);
			if(rhombusKeyspaceDefinition == null) {
				logger.warn("No definition for keyspace {} in Rhombus storage; creating it", keyspaceDefinition.getName());
				addKeyspaceDefinitionToCassandra(keyspaceDefinition);
			} else {
				if(!Objects.equal(keyspaceDefinition, rhombusKeyspaceDefinition)) {
					logger.warn("Keyspace definitions for {} do not match, a migration may be required.", keyspaceDefinition.getName());
				}
				keyspaceDefinition = rhombusKeyspaceDefinition;
			}
			objectMapper = new ObjectMapper(getSessionForKeyspace(keyspaceDefinition), keyspaceDefinition, consistencyHorizon, batchTimeout);
			objectMapper.setLogCql(logCql);
			objectMappers.put(keyspaceName, objectMapper);
		}
		return objectMapper;
	}

	/**
	 * Get an object mapper for a named keyspace.
	 * This will first try to hydrate the keyspace by name from Cassandra.
	 * If unable to do so and the default keyspace matches the name passed in,
	 * the default keyspace definition will be used.
	 * @return Object mapper for the specified keyspace
	 */
	public ObjectMapper getObjectMapper(String keyspaceName) throws Exception {
		CKeyspaceDefinition keyspaceDefinition = null;
		keyspaceDefinition = hydrateLatestKeyspaceDefinitionFromCassandra(keyspaceName);
		if(keyspaceDefinition == null) {
			logger.warn("Attempting to get an object mapper for keyspace {} which does not exist in rhombus storage");
			if(defaultKeyspace != null && defaultKeyspace.getName().equals(keyspaceName)) {
				logger.warn("using default keyspace with same name instead", keyspaceName);
				keyspaceDefinition = defaultKeyspace;
			} else {
				throw new RuntimeException("Attempt to get keyspace by name, but it does not exist in rhombus storage and is not the default keyspace");
			}
		}
		return getObjectMapper(keyspaceDefinition);
	}

	public List<CQLStatement> runMigration(CKeyspaceDefinition newKeyspaceDefinition, boolean executeCql) throws CObjectMigrationException {
		List<CQLStatement> ret = Lists.newArrayList();
		try{
			CKeyspaceDefinition oldKeyspaceDefinition = hydrateLatestKeyspaceDefinitionFromCassandra(newKeyspaceDefinition);
			ObjectMapper om = getObjectMapper(newKeyspaceDefinition);
			om.runMigration(oldKeyspaceDefinition, newKeyspaceDefinition, executeCql);
			if(executeCql) {
				addKeyspaceDefinitionToCassandra(newKeyspaceDefinition);
				om.setKeyspaceDefinition(newKeyspaceDefinition);
			}
		}
		catch(Exception e){
			throw new CObjectMigrationException(e);
		}
		return ret;
	}

	/**
	 * Inserts the json definition of a keyspace into the Rhombus data store
	 * @param keyspaceDefinition Definition to add
	 * @return The inserted keyspace definition
	 */
	public CKeyspaceDefinition addKeyspaceDefinitionToCassandra(CKeyspaceDefinition keyspaceDefinition){
		try{
			com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
			String keyspaceDefinitionAsJson = om.writeValueAsString(keyspaceDefinition);
			getRhombusObjectMapper(keyspaceDefinition).insertKeyspaceDefinition(keyspaceDefinition.getName(), keyspaceDefinitionAsJson);
		} catch(Exception e){
			logger.error("Unable to add keyspace definition {} to cassandra", keyspaceDefinition.getName(), e);
		}
		return keyspaceDefinition;
	}

	/**
	 * Get the latest version of a keyspace definition from the Rhombus data store
	 * @param keyspaceDefinition Keyspace definition
	 * @return hydrated keyspace
	 */
	public CKeyspaceDefinition hydrateLatestKeyspaceDefinitionFromCassandra(CKeyspaceDefinition keyspaceDefinition){
		return getRhombusObjectMapper(keyspaceDefinition).hydrateRhombusKeyspaceDefinition(keyspaceDefinition.getName());
	}

	/**
	 * Get the latest version of a keyspace definition from the Rhombus data store
	 * @param keyspaceName Name of keyspace
	 * @return hydrated keyspace
	 */
	public CKeyspaceDefinition hydrateLatestKeyspaceDefinitionFromCassandra(String keyspaceName) {
		return getRhombusObjectMapper(defaultKeyspace).hydrateRhombusKeyspaceDefinition(keyspaceName);
	}

	/**
	 * This method rebuilds a keyspace from a definition.  If forceRebuild is true, the process
	 * removes any existing keyspace with the same name.  This operation is immediate and irreversible.
	 *
	 * @param keyspaceDefinition The definition to build the keyspace from
	 * @param forceRebuild Force destruction and rebuild of keyspace
	 */
	public void buildKeyspace(CKeyspaceDefinition keyspaceDefinition, Boolean forceRebuild) throws Exception {
		if(keyspaceDefinition == null) {
			keyspaceDefinition = defaultKeyspace;
		}

		// Get a session for the new keyspace
		Session session = getSessionForKeyspace(keyspaceDefinition);

		// Use this session to create an object mapper and build the keyspace
		ObjectMapper mapper = new ObjectMapper(session, keyspaceDefinition, consistencyHorizon, batchTimeout);
		mapper.setLogCql(logCql);
		mapper.buildKeyspace(forceRebuild);
		mapper.prePrepareInsertStatements();

		// Insert the keyspace definition into the Rhombus data store
		addKeyspaceDefinitionToCassandra(keyspaceDefinition);

		// Add the object mapper to our cache
		objectMappers.put(keyspaceDefinition.getName(), mapper);
	}

	/**
	 * Some tests require a hard keyspace drop.  This is a dangerous and expensive operation.
	 * Use this sparingly and with caution.
	 * @param keyspaceName Name of keyspace to remove
	 */
	public void dropKeyspace(String keyspaceName) {
		Session session = cluster.connect();
		//First try to create the new keyspace
		StringBuilder sb = new StringBuilder();
		sb.append("DROP KEYSPACE ");
		sb.append(keyspaceName);
		sb.append(";");
		try {
			session.execute(sb.toString());
		} catch(Exception e) {
			logger.warn("Unable to drop keyspace {}", keyspaceName, e);
		}
		session.shutdown();
		if(objectMappers.containsKey(keyspaceName)) {
			objectMappers.remove(keyspaceName);
		}
	}


	/**
	 * Create and return a new session for the specified cluster.
	 * The caller is responsible for terminating the session.
	 * @return Empty session
	 */
	public Session getEmptySession() {
		return cluster.connect();
	}

	private Session getSessionForKeyspace(CKeyspaceDefinition keyspace) throws Exception {
		createKeyspaceIfNotExists(keyspace.getName(), keyspace, true);
		return cluster.connect(keyspace.getName());
	}

	/**
	 * Tear down all connections contained in associated object mappers
	 * and shutdown the cluster.
	 */
	public void teardown() {
		for(ObjectMapper mapper : objectMappers.values()) {
			mapper.teardown();
		}
		cluster.shutdown();
	}

	public void setDefaultKeyspace(CKeyspaceDefinition keyspaceDefinition) {
		this.defaultKeyspace = keyspaceDefinition;
	}

	public boolean isLogCql() {
		return logCql;
	}

	public void setLogCql(boolean logCql) {
		this.logCql = logCql;
	}

	public Integer getNativeTransportPort() {
		return nativeTransportPort;
	}

	public void setNativeTransportPort(Integer nativeTransportPort) {
		this.nativeTransportPort = nativeTransportPort;
	}

	public String getRhombusKeyspaceName() {
		return rhombusKeyspaceName;
	}

	public void setRhombusKeyspaceName(String rhombusKeyspaceName) {
		this.rhombusKeyspaceName = rhombusKeyspaceName;
	}

	public LoadBalancingPolicy getLoadBalancingPolicy() {
		return loadBalancingPolicy;
	}

	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

}
