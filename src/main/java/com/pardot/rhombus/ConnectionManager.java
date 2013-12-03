package com.pardot.rhombus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.collect.Maps;

import com.pardot.rhombus.cobject.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.SocketOptions;
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

	public LoadBalancingPolicy getLoadBalancingPolicy() {
		return loadBalancingPolicy;
	}

	public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
		this.loadBalancingPolicy = loadBalancingPolicy;
	}

	/**
	 * Get the default object mapper
	 * @return The default object mapper
	 */
	public ObjectMapper getObjectMapper() {
		return getObjectMapper(defaultKeyspace.getName());
	}

	/**
	 * Get an object mapper for a keyspace
	 * @return Object mapper for the specified keyspace
	 */
	public ObjectMapper getObjectMapper(String keyspace) {
		ObjectMapper objectMapper = objectMappers.get(keyspace);
		if(objectMapper == null) {
			Session session = cluster.connect(keyspace);
			defaultKeyspace = hydrateLatestKeyspaceDefinitionFromCassandra(keyspace, session);
			logger.debug("Connecting to keyspace {}", defaultKeyspace.getName());
			objectMapper = new ObjectMapper(session, defaultKeyspace, consistencyHorizon, batchTimeout);
			objectMapper.setLogCql(logCql);
			objectMappers.put(keyspace, objectMapper);
		}
		return objectMapper;

	}

	public CKeyspaceDefinition hydrateLatestKeyspaceDefinitionFromCassandra(String keyspaceName, Session session){
		try{
			CQLStatement cql = CObjectCQLGenerator.makeCQLforGetKeyspaceDefinitions(keyspaceName);
			CQLExecutor executor = new CQLExecutor(session, false, null);
			com.datastax.driver.core.ResultSet r = executor.executeSync(cql);
			return CKeyspaceDefinition.fromJsonString(r.one().getString("def"));
		}
		catch(Exception e){
			logger.error("Unable to hydrate keyspace definition from cassandra");
		}
		return null;
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
		//Get a session for the new keyspace
		Session session = getSessionForNewKeyspace(keyspaceDefinition, forceRebuild);
		//Use this session to create an object mapper and build the keyspace
		ObjectMapper mapper = new ObjectMapper(session, keyspaceDefinition, consistencyHorizon, batchTimeout);
		mapper.setLogCql(logCql);
		mapper.buildKeyspace(forceRebuild);
		mapper.prePrepareInsertStatements();
		objectMappers.put(keyspaceDefinition.getName(), mapper);
	}

	/**
	 * Create and return a new session for the specified cluster.
	 * The caller is responsible for terminating the session.
	 * @return Empty session
	 */
	public Session getEmptySession() {
		return cluster.connect();
	}

	private Session getSessionForNewKeyspace(CKeyspaceDefinition keyspace, Boolean forceRebuild) throws Exception {
		//Get a new session
		Session session = cluster.connect();

		if(forceRebuild) {
			try {
				//Drop the keyspace if it already exists
				String cql = "DROP KEYSPACE " + keyspace.getName() + ";";
				if(this.isLogCql()) {
					logger.debug("Executing CQL: {}", cql);
				}
				session.execute(cql);
			} catch(Exception e) {
				logger.debug("Exception executing drop keyspace cql", e);
			}
		}

		//First try to create the new keyspace
		StringBuilder sb = new StringBuilder();
		sb.append(keyspace.getName());
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
		} catch(Exception e) {
			//TODO Catch only the specific exception for keyspace already exists
			if(!forceRebuild) {
				//If we are not forcing a rebuild and the create failed, attempt to update
				session.execute("ALTER KEYSPACE " + sb.toString());
			} else {
				throw e;
			}
		}

		//Close our session and get a new one directly associated with the new keyspace
		session.shutdown();
		session = cluster.connect(keyspace.getName());
		return session;
	}

	/**
	 * Teardown all connections contained in associated object mappers
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
}
