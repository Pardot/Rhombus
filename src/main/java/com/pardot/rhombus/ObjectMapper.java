package com.pardot.rhombus;


import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.async.StatementIteratorConsumer;
import com.pardot.rhombus.util.JsonUtil;
import com.yammer.metrics.*;
import com.yammer.metrics.core.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 4/17/13
 */
public class ObjectMapper implements CObjectShardList {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapper.class);
	private static final int reasonableStatementLimit = 20;
	private boolean executeAsync = true;
	private boolean logCql = false;
	private boolean cacheBoundedQueries = true;
	private CQLExecutor cqlExecutor;
	private Session session;
	private CKeyspaceDefinition keyspaceDefinition;
	private CObjectCQLGenerator cqlGenerator;
	private Long batchTimeout;

	public ObjectMapper(Session session, CKeyspaceDefinition keyspaceDefinition, Integer consistencyHorizon, Long batchTimeout) {
		this.cqlExecutor = new CQLExecutor(session, logCql, keyspaceDefinition.getConsistencyLevel());
		this.session = session;
		this.keyspaceDefinition = keyspaceDefinition;
		this.cqlGenerator = new CObjectCQLGenerator(keyspaceDefinition.getName(), keyspaceDefinition.getDefinitions(), this, consistencyHorizon);
		this.batchTimeout = batchTimeout;
	}

	/**
	 * Build the tables contained in the keyspace definition.
	 * This method assumes that its keyspace exists and
	 * does not contain any tables.
	 */
	public void buildKeyspace(Boolean forceRebuild) {
		//we are about to rework the the keyspaces, so lets clear the bounded query cache
		cqlExecutor.clearStatementCache();
		//First build the shard index
		CQLStatement cql = CObjectCQLGenerator.makeCQLforShardIndexTableCreate();
		try {
			cqlExecutor.executeSync(cql);
		} catch(Exception e) {
			if(forceRebuild) {
				CQLStatement dropCql = CObjectCQLGenerator.makeCQLforShardIndexTableDrop();
				logger.debug("Attempting to drop table with cql {}", dropCql);
				cqlExecutor.executeSync(dropCql);
				cqlExecutor.executeSync(cql);
			} else {
				logger.debug("Not dropping shard index table");
			}
		}
		//Next build the update index
		cql = CObjectCQLGenerator.makeCQLforIndexUpdateTableCreate();
		try{
			cqlExecutor.executeSync(cql);
		}
		catch(Exception e) {
			logger.debug("Unable to create update index table. It may already exist");
		}
		//Now build the tables for each object if the definition contains tables
		if(keyspaceDefinition.getDefinitions() != null) {
			for(CDefinition definition : keyspaceDefinition.getDefinitions().values()) {
				CQLStatementIterator statementIterator = cqlGenerator.makeCQLforCreate(definition.getName());
				CQLStatementIterator dropStatementIterator = cqlGenerator.makeCQLforDrop(definition.getName());
				while(statementIterator.hasNext()) {
					cql = statementIterator.next();
					CQLStatement dropCql = dropStatementIterator.next();
					try {
						cqlExecutor.executeSync(cql);
					} catch (AlreadyExistsException e) {
						if(forceRebuild) {
							logger.debug("ForceRebuild is on, dropping table");
							cqlExecutor.executeSync(dropCql);
							cqlExecutor.executeSync(cql);
						} else {
							logger.warn("Table already exists and will not be updated");
						}
					}
				}
			}
		}
	}

	public UUID getTimeUUIDAtEndOfConsistencyHorizion(){
		return cqlGenerator.getTimeUUIDAtEndOfConsistencyHorizion();
	}

	public void executeStatements(CQLStatementIterator statementIterator) throws RhombusException {
		List<CQLStatementIterator> statementIterators = Lists.newArrayList();
		statementIterators.add(statementIterator);
		executeStatements(statementIterators);
	}

	public void executeStatements(List<CQLStatementIterator> statementIterators) throws RhombusException {
		boolean canExecuteAsync = true;
		for(CQLStatementIterator statementIterator : statementIterators) {
			if(!statementIterator.isBounded()) {
				canExecuteAsync = false;
				break;
			}
		}
		if(canExecuteAsync &&  this.executeAsync) {
			logger.debug("Executing statements async");
			//If this is a bounded statement iterator, send it through the async path
			long start = System.nanoTime();
			List<StatementIteratorConsumer> consumers = Lists.newArrayList();
			for(CQLStatementIterator statementIterator : statementIterators) {
				StatementIteratorConsumer consumer = new StatementIteratorConsumer((BoundedCQLStatementIterator) statementIterator, cqlExecutor, batchTimeout);
				consumer.start();
				consumers.add(consumer);
			}
			for(StatementIteratorConsumer consumer : consumers) {
				consumer.join();
			}
			logger.debug("Async execution took {}us", (System.nanoTime() - start) / 1000);
		} else {
			logger.debug("Executing statements sync");
			long start = System.nanoTime();
			for(CQLStatementIterator statementIterator : statementIterators) {
				while(statementIterator.hasNext()) {
					CQLStatement statement = statementIterator.next();
					final com.yammer.metrics.core.Timer syncSingleExecTimer = com.yammer.metrics.Metrics.defaultRegistry().newTimer(ObjectMapper.class, "syncSingleExec");
					final TimerContext syncSingleExecTimerContext = syncSingleExecTimer.time();
					cqlExecutor.executeSync(statement);
					syncSingleExecTimerContext.stop();
				}
			}
			logger.debug("Sync execution took {} ms", (System.nanoTime() - start) / 1000000);
		}
	}

	@Override
	public List<Long> getShardIdList(CDefinition def, SortedMap<String, Object> indexValues, CObjectOrdering ordering, @Nullable UUID start, @Nullable UUID end) throws CQLGenerationException {
		CQLStatement shardIdGet = CObjectCQLGenerator.makeCQLforGetShardIndexList(this.keyspaceDefinition.getName(), def, indexValues, ordering, start, end);
		ResultSet resultSet = cqlExecutor.executeSync(shardIdGet);
		List<Long> shardIdList = Lists.newArrayList();
		for(Row row : resultSet) {
			shardIdList.add(row.getLong("shardid"));
		}
		return shardIdList;
	}

	/**
	 * Insert a batch of mixed new object with values
	 * @param objects Objects to insert
	 * @return ID of most recently inserted object
	 * @throws CQLGenerationException
	 */
	public UUID insertBatchMixed(Map<String, List<Map<String, Object>>> objects) throws CQLGenerationException, RhombusException {
		logger.debug("Insert batch mixed");
		List<CQLStatementIterator> statementIterators = Lists.newArrayList();
		UUID key = null;
		for(String objectType : objects.keySet()) {
			for(Map<String, Object> values : objects.get(objectType)) {
				key = UUIDs.timeBased();
				long timestamp = System.currentTimeMillis();
				CQLStatementIterator statementIterator = cqlGenerator.makeCQLforInsert(objectType, values, key, timestamp);
				statementIterators.add(statementIterator);
			}
		}
		executeStatements(statementIterators);
		return key;
	}

	/**
	 * Insert a new object with values and key
	 * @param objectType Type of object to insert
	 * @param values Values to insert
	 * @param key Time UUID to use as key
	 * @return ID if newly inserted object
	 * @throws CQLGenerationException
	 */
	public UUID insert(String objectType, Map<String, Object> values, UUID key) throws CQLGenerationException, RhombusException {
		logger.debug("Insert {}", objectType);
		if(key == null) {
			key = UUIDs.timeBased();
		}
		long timestamp = System.currentTimeMillis();
		CQLStatementIterator statementIterator = cqlGenerator.makeCQLforInsert(objectType, values, key, timestamp);
		executeStatements(statementIterator);
		return key;
	}

	/**
	 * Insert a new objectType with values
	 * @param objectType Type of object to insert
	 * @param values Values to insert
	 * @return UUID of inserted object
	 * @throws CQLGenerationException
	 */
	public UUID insert(String objectType, Map<String, Object> values) throws CQLGenerationException, RhombusException {
		return insert(objectType, values, (UUID)null);
	}

	/**
	 * Used to insert an object with a UUID based on the provided timestamp
	 * Best used for testing, as time resolution collisions are not accounted for
	 * @param objectType Type of object to insert
	 * @param values Values to insert
	 * @param timestamp Timestamp to use to create the object UUID
	 * @return the UUID of the newly inserted object
	 */
	public UUID insert(String objectType, Map<String, Object> values, Long timestamp) throws CQLGenerationException, RhombusException {
		UUID uuid = UUIDs.startOf(timestamp);
		return insert(objectType, values, uuid);
	}

	/**
	 * Delete Object of type with id key
	 * @param objectType Type of object to delete
	 * @param key Key of object to delete
	 */
	public void delete(String objectType, UUID key) {
		CDefinition def = keyspaceDefinition.getDefinitions().get(objectType);
		Map<String, Object> values = getByKey(objectType, key);
		CQLStatementIterator statementIterator = cqlGenerator.makeCQLforDelete(objectType, key, values, null);
		mapResults(statementIterator, def, 0L);
	}

	public void deleteObsoleteIndex(IndexUpdateRow row, CIndex index, Map<String,Object> indexValues){
		Statement cql = cqlGenerator.makeCQLforDeleteUUIDFromIndex_WorkaroundForUnpreparableTimestamp(
			this.keyspaceDefinition.getName(),
			keyspaceDefinition.getDefinitions().get(row.getObjectName()),
			index,
			row.getInstanceId(),
			index.getIndexKeyAndValues(indexValues),
			row.getTimeStampOfMostCurrentUpdate());
		cqlExecutor.executeSync(cql);
	}

	public void deleteObsoleteUpdateIndexColumn(IndexUpdateRowKey rowKey, UUID id){
		CQLStatement cql = cqlGenerator.makeCQLforDeleteObsoleteUpdateIndexColumn(rowKey, id);
		cqlExecutor.executeSync(cql);
	}

	/**
	 * Update objectType with key using values
	 * @param objectType Type of object to update
	 * @param key Key of object to update
	 * @param values Values to update
	 * @param timestamp Timestamp to execute update at
	 * @param ttl Time to live for update
	 * @return new UUID of the object
	 * @throws CQLGenerationException
	 */
	public UUID update(String objectType, UUID key, Map<String, Object> values, Long timestamp, Integer ttl) throws CQLGenerationException, RhombusException {
		//New Version
		//(1) Get the old version
		Map<String, Object> oldversion = getByKey(objectType, key);

		//(2) Pass it all into the cql generator so it can create the right statements
		CDefinition def = keyspaceDefinition.getDefinitions().get(objectType);
		CQLStatementIterator statementIterator = cqlGenerator.makeCQLforUpdate(keyspaceDefinition.getName(), def, key, oldversion, values);
		executeStatements(statementIterator);
		return key;
	}

	/**
	 * Update objectType with key using values
	 * @param objectType Type of object to update
	 * @param key Key of object to update
	 * @param values Values to update
	 * @return new UUID of the object
	 * @throws CQLGenerationException
	 */
	public UUID update(String objectType, UUID key, Map<String, Object> values) throws CQLGenerationException, RhombusException {
		return update(objectType, key, values, null, null);
	}

	/**
	 *
	 * @param objectType Type of object to get
	 * @param key Key of object to get
	 * @return Object of type with key or null if it does not exist
	 */
	public Map<String, Object> getByKey(String objectType, UUID key) {
		CDefinition def = keyspaceDefinition.getDefinitions().get(objectType);
		CQLStatementIterator statementIterator = cqlGenerator.makeCQLforGet(objectType, key);
		List<Map<String, Object>> results = mapResults(statementIterator, def, 1L);
		if(results.size() > 0) {
			return results.get(0);
		} else {
			return null;
		}
	}

	/**
	 * @param objectType Type of object to query
	 * @param criteria Criteria to query by
	 * @return List of objects that match the specified type and criteria
	 * @throws CQLGenerationException
	 */
	public List<Map<String, Object>> list(String objectType, Criteria criteria) throws CQLGenerationException {
		CDefinition def = keyspaceDefinition.getDefinitions().get(objectType);
		CQLStatementIterator statementIterator = cqlGenerator.makeCQLforGet(objectType, criteria);
		return mapResults(statementIterator, def, criteria.getLimit());
	}

	protected SortedMap<String,Object> unpackIndexValuesFromJson(CDefinition def, String json) throws IOException, JsonMappingException {
		org.codehaus.jackson.map.ObjectMapper om = new org.codehaus.jackson.map.ObjectMapper();
		TreeMap<String,Object> jsonMap = om.readValue(json, TreeMap.class);
		return JsonUtil.rhombusMapFromJsonMap(jsonMap,def);
	}

	public IndexUpdateRow getNextUpdateIndexRow(@Nullable IndexUpdateRowKey lastInstanceKey) throws IOException, JsonMappingException {
		CQLStatement cqlForNext = (lastInstanceKey == null) ?
			cqlGenerator.makeGetFirstEligibleIndexUpdate() : cqlGenerator.makeGetNextEligibleIndexUpdate(lastInstanceKey);
		ResultSet resultSet = cqlExecutor.executeSync(cqlForNext);
		if(resultSet.isExhausted()){
			return null;
		}
		IndexUpdateRowKey nextInstanceKey = new IndexUpdateRowKey(resultSet.one());
		CQLStatement cqlForRow = cqlGenerator.makeGetRowIndexUpdate(keyspaceDefinition.getName(), nextInstanceKey);
		resultSet = cqlExecutor.executeSync(cqlForRow);
		List<Row> results = resultSet.all();
		if(results.size() == 0 ){
			return null;
		}
		String objectName = results.get(0).getString("statictablename");
		CDefinition def = keyspaceDefinition.getDefinitions().get(objectName);

		List<SortedMap<String,Object>> indexValueList = Lists.newArrayList();
		List<UUID> ids = Lists.newArrayList();
		for(Row update : results){
			indexValueList.add(unpackIndexValuesFromJson(def,update.getString("indexvalues")));
			ids.add(update.getUUID("id"));
		}


		return new IndexUpdateRow(
			objectName,
			results.get(0).getUUID("instanceid"),
			UUIDs.unixTimestamp(results.get(0).getUUID("id"))*1000,
			indexValueList,
			ids
		);
	}


	/**
	 * Iterates through cql statements executing them in sequence and mapping the results until limit is reached
	 * @param statementIterator Statement iterator to execute
	 * @param definition definition to execute the statements against
	 * @return Ordered resultset concatenating results from statements in statement iterator.
	 */
	private List<Map<String, Object>> mapResults(CQLStatementIterator statementIterator, CDefinition definition, Long limit) {
		List<Map<String, Object>> results = Lists.newArrayList();
		int statementNumber = 0;
		int resultNumber = 0;
		while(statementIterator.hasNext(resultNumber) ) {
			CQLStatement cql = statementIterator.next();
			ResultSet resultSet = cqlExecutor.executeSync(cql);
			for(Row row : resultSet) {
				Map<String, Object> result = mapResult(row, definition);
				results.add(result);
				resultNumber++;
			}
			statementNumber++;
			if((limit > 0 && resultNumber >= limit) || statementNumber > reasonableStatementLimit) {
				logger.debug("Breaking from mapping results");
				break;
			}
		}
		return results;
	}

	/**
	 * @param row The row to map
	 * @param definition The definition to map the row on to
	 * @return Data contained in a row mapped to the object described in definition.
	 */
	private Map<String, Object> mapResult(Row row, CDefinition definition) {
		Map<String, Object> result = Maps.newHashMap();
		result.put("id", row.getUUID("id"));
		for(CField field : definition.getFields().values()) {
			result.put(field.getName(), getFieldValue(row, field));
		}
		return result;
	}

    public void prePrepareInsertStatements() throws CQLGenerationException {
        Map<String,CDefinition> defs = this.getKeyspaceDefinition().getDefinitions();
		if(defs != null) {
			for(CDefinition def : defs.values()){
				prePrepareInsertStatements(def);
			}
		}
    }

    public void prePrepareInsertStatements(CDefinition def) throws CQLGenerationException{
        SortedMap<String, Object> values = Maps.newTreeMap();
        for( CField f : def.getFields().values() ){
            values.put(f.getName(), f.getEmptyJavaObjectOfThisType());
        }
        CQLStatementIterator sti = cqlGenerator.makeCQLforInsert(def.getName(),values,UUIDs.timeBased(),0L);
        while(sti.hasNext()){
            CQLStatement cql = sti.next();
            cqlExecutor.prepareStatement(session,cql);
        }
    }

	private Object getFieldValue(Row row, CField field) {
		Object fieldValue;
		switch(field.getType()) {
			case ASCII:
			case VARCHAR:
			case TEXT:
				fieldValue = row.getString(field.getName());
				break;
			case BIGINT:
			case COUNTER:
				fieldValue = row.getLong(field.getName());
				break;
			case BLOB:
				fieldValue = row.getBytes(field.getName());
				break;
			case BOOLEAN:
				fieldValue = row.getBool(field.getName());
				break;
			case DECIMAL:
				fieldValue = row.getDecimal(field.getName());
				break;
			case DOUBLE:
				fieldValue = row.getDouble(field.getName());
				break;
			case FLOAT:
				fieldValue = row.getFloat(field.getName());
				break;
			case INT:
				fieldValue = row.getInt(field.getName());
				break;
			case TIMESTAMP:
				fieldValue = row.getDate(field.getName());
				break;
			case UUID:
			case TIMEUUID:
				fieldValue = row.getUUID(field.getName());
				break;
			case VARINT:
				fieldValue = row.getVarint(field.getName());
				break;
			default:
				fieldValue = null;
		}
		return (fieldValue == null ? null : fieldValue);
	}

	public Map<String, Object> coerceRhombusValuesFromJsonMap(String objectType, Map<String, Object> values) {
		return JsonUtil.rhombusMapFromJsonMap(values, keyspaceDefinition.getDefinitions().get(objectType));
	}

	public void setLogCql(boolean logCql) {
		this.logCql = logCql;
		this.cqlExecutor.setLogCql(logCql);
	}

	public boolean getExecuteAsync() {
		return executeAsync;
	}

	public void setExecuteAsync(boolean executeAsync) {
		logger.debug("{} setting executeAsync to {}", this.keyspaceDefinition.getName(), executeAsync);
		this.executeAsync = executeAsync;
	}

	public void teardown() {
		session.shutdown();
	}

	public boolean isCacheBoundedQueries() {
		return cacheBoundedQueries;
	}

	public void setCacheBoundedQueries(boolean cacheBoundedQueries) {
		this.cacheBoundedQueries = cacheBoundedQueries;
	}

    public void setCompaction(String strategy, Map<String,Object> options) throws CQLGenerationException, RhombusException {
        CQLStatementIterator cql = cqlGenerator.makeCQLforCompaction(keyspaceDefinition, strategy, options);
        executeStatements(cql);
    }

	public CQLExecutor getCqlExecutor(){
		return cqlExecutor;
	}

	public CObjectCQLGenerator getCqlGenerator_ONLY_FOR_TESTING(){
		return cqlGenerator;
	}

	public CKeyspaceDefinition getKeyspaceDefinition_ONLY_FOR_TESTING(){
		return keyspaceDefinition;
	}

	protected CKeyspaceDefinition getKeyspaceDefinition() {
		return keyspaceDefinition;
	}

	public CDefinition getDefinition(String objectType) {
		return keyspaceDefinition.getDefinitions().get(objectType);
	}

}
