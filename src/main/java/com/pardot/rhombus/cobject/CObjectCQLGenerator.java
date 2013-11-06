package com.pardot.rhombus.cobject;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Using;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.shardingstrategy.ShardStrategyException;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.*;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/8/13
 */
public class CObjectCQLGenerator {

	private static Logger logger = LoggerFactory.getLogger(CObjectCQLGenerator.class);

	protected static final String TEMPLATE_CREATE_STATIC = "CREATE TABLE \"%s\" (id timeuuid PRIMARY KEY, %s);";
	protected static final String TEMPLATE_CREATE_WIDE = "CREATE TABLE \"%s\" (id timeuuid, shardid bigint, %s, PRIMARY KEY ((shardid, %s),id) );";
	protected static final String TEMPLATE_CREATE_KEYSPACE_LIST = "CREATE TABLE \"__keyspace_definitions\" (id timeuuid, shardid bigint, def varchar, PRIMARY KEY ((shardid),id) );";
	protected static final String TEMPLATE_CREATE_WIDE_INDEX = "CREATE TABLE \"%s\" (shardid bigint, tablename varchar, indexvalues varchar, targetrowkey varchar, PRIMARY KEY ((tablename, indexvalues),shardid) );";
	protected static final String TEMPLATE_CREATE_INDEX_UPDATES = "CREATE TABLE \"__index_updates\" (id timeuuid, statictablename varchar, instanceid timeuuid, indexvalues varchar, PRIMARY KEY ((statictablename,instanceid),id))";
	protected static final String TEMPLATE_DROP = "DROP TABLE %s.\"%s\";";
	protected static final String TEMPLATE_INSERT_STATIC = "INSERT INTO %s.\"%s\" (%s) VALUES (%s)%s;";//"USING TIMESTAMP %s%s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_WIDE = "INSERT INTO %s.\"%s\" (%s) VALUES (%s)%s;";//"USING TIMESTAMP %s%s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_KEYSPACE = "INSERT INTO %s.\"__keyspace_definitions\" (id, shardid, def) values (?, ?, ?);";
	protected static final String TEMPLATE_INSERT_WIDE_INDEX = "INSERT INTO %s.\"%s\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);";//"USING TIMESTAMP %s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_INDEX_UPDATES = "INSERT INTO %s.\"__index_updates\" (id, statictablename, instanceid, indexvalues) values (?, ?, ?, ?);";
	protected static final String TEMPLATE_SELECT_STATIC = "SELECT * FROM %s.\"%s\" WHERE %s;";
	protected static final String TEMPLATE_SELECT_WIDE = "SELECT %s FROM %s.\"%s\" WHERE shardid = %s AND %s ORDER BY id %s %s ALLOW FILTERING;";
	protected static final String TEMPLATE_SELECT_KEYSPACES = "SELECT * FROM %s.\"__keyspace_definitions\" WHERE shardid = 1 ORDER BY id DESC;";
	protected static final String TEMPLATE_SELECT_WIDE_INDEX = "SELECT shardid FROM %s.\"%s\" WHERE tablename = ? AND indexvalues = ?%s ORDER BY shardid %s ALLOW FILTERING;";
	protected static final String TEMPLATE_DELETE = "DELETE FROM %s.%s WHERE %s;";//"DELETE FROM %s USING TIMESTAMP %s WHERE %s;"; //Add back when timestamps become preparable
	protected static final String TEMPLATE_DELETE_OBSOLETE_UPDATE_INDEX_COLUMN = "DELETE FROM %s.\"__index_updates\" WHERE  statictablename = ? and instanceid = ? and id = ?";
	protected static final String TEMPLATE_SELECT_FIRST_ELIGIBLE_INDEX_UPDATE = "SELECT statictablename,instanceid FROM %s.\"__index_updates\" WHERE id < ? limit 1 allow filtering;";
	protected static final String TEMPLATE_SELECT_NEXT_ELIGIBLE_INDEX_UPDATE = "SELECT statictablename,instanceid FROM %s.\"__index_updates\" where token(statictablename,instanceid) > token(?,?) and id < ? limit 1 allow filtering;";
	protected static final String TEMPLATE_SELECT_ROW_INDEX_UPDATE = "SELECT * FROM %s.\"__index_updates\" where statictablename = ? and instanceid = ? order by id DESC;";
	protected static final String TEMPLATE_SET_COMPACTION_LEVELED = "ALTER TABLE %s.\"%s\" WITH compaction = { 'class' :  'LeveledCompactionStrategy',  'sstable_size_in_mb' : %d }";
	protected static final String TEMPLATE_SET_COMPACTION_TIERED = "ALTER TABLE %s.\"%s\" WITH compaction = { 'class' :  'SizeTieredCompactionStrategy',  'min_threshold' : %d }";
	protected static final String TEMPLATE_TABLE_SCAN = "SELECT * FROM %s.\"%s\";";

	protected Map<String, CDefinition> definitions;
	protected CObjectShardList shardList;
	private Integer consistencyHorizon;
	private String keyspace;

	/**
	 * Single Param constructor, mostly for testing convenience. Use the other constructor.
	 */
	public CObjectCQLGenerator(String keyspace, Integer consistencyHorizon){
		this.definitions = Maps.newHashMap();
		this.consistencyHorizon = consistencyHorizon;
		this.keyspace = keyspace;
	}


	/**
	 *
	 * @param objectDefinitions - A map where the key is the CDefinition.name and the value is the CDefinition.
	 *                          This map should include a CDefinition for every object in the system.
	 */
	public CObjectCQLGenerator(String keyspace, Map<String, CDefinition> objectDefinitions, CObjectShardList shardList, Integer consistencyHorizon){
		this.definitions = objectDefinitions;
		this.consistencyHorizon = consistencyHorizon;
        this.keyspace = keyspace;
        setShardList(shardList);
	}

	/**
	 * Set the Definitions to be used
	 * @param objectDefinitions - A map where the key is the CDefinition.name and the value is the CDefinition.
	 *                          This map should include a CDefinition for every object in the system.
	 */
	public void setDefinitions(Map<String, CDefinition> objectDefinitions){
		this.definitions = objectDefinitions;
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	public CQLStatementIterator makeCQLforCreate(String objType){
		return makeCQLforCreate(this.definitions.get(objType));
	}

	/**
	 *
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	public CQLStatement makeCQLforCreateKeyspaceDefinitionsTable(){
		return CQLStatement.make(TEMPLATE_CREATE_KEYSPACE_LIST);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	public CQLStatementIterator makeCQLforDrop(String objType){
		return makeCQLforDrop(this.keyspace, this.definitions.get(objType));
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param data - A map of fieldnames to values representing the data to insert
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	@NotNull
	public CQLStatementIterator makeCQLforInsert(String objType, Map<String,Object> data) throws CQLGenerationException {
		return makeCQLforInsert(this.keyspace, this.definitions.get(objType), data);
	}

	/**
	 *
	 * @param keyspaceDefinition - The JSON keyspace definition
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	@NotNull
	public CQLStatementIterator makeCQLforInsertKeyspaceDefinition(String keyspaceDefinition) throws CQLGenerationException {
		return makeCQLforInsertKeyspaceDefinition(this.keyspace,keyspaceDefinition,UUIDs.timeBased());
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param data - A map of fieldnames to values representing the data to insert
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	@NotNull
	public CQLStatementIterator makeCQLforInsert(String objType, Map<String,Object> data, UUID key, Long timestamp) throws CQLGenerationException {
		return makeCQLforInsert(this.keyspace, this.definitions.get(objType), data, key, timestamp, 0);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param key - The TimeUUID of the object to retrieve
	 * @return Iterator of CQL statements that need to be executed for this task. (Should have a length of 1 for this particular method)
	 */
	@NotNull
	public CQLStatementIterator makeCQLforGet(String objType, UUID key){
		return makeCQLforGet(this.keyspace, this.definitions.get(objType), key);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @return Iterator of CQL statements that need to be executed for this task. (Should have a length of 1 for this particular method)
	 */
	@NotNull
	public CQLStatement makeCQLforTableScan(String objType){
		return makeCQLforTableScan(this.keyspace, this.definitions.get(objType));
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param criteria - The criteria object describing which rows to retrieve
	 * @param countOnly - true means you want a count of rows, false means you want the rows themselves
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	@NotNull
	public CQLStatementIterator makeCQLforGet(String objType, Criteria criteria, boolean countOnly) throws CQLGenerationException {
		CDefinition definition = this.definitions.get(objType);
		CObjectOrdering ordering = (criteria.getOrdering() != null ? criteria.getOrdering(): CObjectOrdering.DESCENDING);
		UUID endUuid = (criteria.getEndUuid() == null ? UUIDs.startOf(DateTime.now().getMillis()) : criteria.getEndUuid());
		return makeCQLforGet(this.keyspace, shardList, definition, criteria.getIndexKeys(), ordering,  criteria.getStartUuid(),
				 endUuid, criteria.getLimit(), criteria.getInclusive(), countOnly);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param criteria - The criteria object describing which rows to retrieve
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	@NotNull
	public CQLStatementIterator makeCQLforGet(String objType, Criteria criteria) throws CQLGenerationException {
		return makeCQLforGet( objType, criteria, false);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param criteria - The criteria object describing which rows to count
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	@NotNull
	public CQLStatementIterator makeCQLforCount(String objType, Criteria criteria) throws CQLGenerationException {
		return makeCQLforGet( objType, criteria, true);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param indexkeys - A map of fieldnames to values representing the where clause of the index query
	 * @param ordering - CObjectOrdering.ASCENDING or CObjectOrdering.DESCENDING
	 * @param start - UUID of the item before the first result
	 * @param end - UUID of the item after the first result (Assuming the limit doesnt override it)
	 * @param limit - The maximum number of results
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	@NotNull
	public CQLStatementIterator makeCQLforGet(String objType, SortedMap<String,Object> indexkeys,CObjectOrdering ordering,@Nullable UUID start, @Nullable UUID end, long limit) throws CQLGenerationException {
		return makeCQLforGet(this.keyspace, this.shardList, this.definitions.get(objType), indexkeys,ordering,start,end,limit, false);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param indexkeys - A map of fieldnames to values representing the where clause of the index query
	 * @param limit - The maximum number of results
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	public CQLStatementIterator makeCQLforGet(String objType, SortedMap<String,Object> indexkeys, Long limit) throws CQLGenerationException {
		return makeCQLforGet(this.keyspace, this.shardList, this.definitions.get(objType), indexkeys,limit);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param indexkeys - A map of fieldnames to values representing the where clause of the index query
	 * @param ordering - CObjectOrdering.ASCENDING or CObjectOrdering.DESCENDING
	 * @param starttimestamp - Return results equal to or after this timestamp
	 * @param endtimestamp - Return results equal to or before this timestamp
	 * @param limit - The maximum number of results
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	public CQLStatementIterator makeCQLforGet(String objType, SortedMap<String,Object> indexkeys, CObjectOrdering ordering, Long starttimestamp, Long endtimestamp, Long limit) throws CQLGenerationException {
		return makeCQLforGet(this.keyspace, this.shardList, this.definitions.get(objType), indexkeys,ordering, starttimestamp, endtimestamp, limit);
	}

	/**
	 *
	 * @return an iterator for getting all the keyspace definitions
	 */
	public CQLStatement makeCQLforGetKeyspaceDefinitions(){
		return makeCQLforGetKeyspaceDefinitions(this.keyspace);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param key - The TimeUUID of the object to delete
	 * @param data - All the values of the fields existing in this object (or just the required fields will work)
	 * @param timestamp - The timestamp for the request
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	@NotNull
	public CQLStatementIterator makeCQLforDelete(String objType, UUID key,  Map<String,Object> data, Long timestamp){
		return makeCQLforDelete(this.keyspace, this.definitions.get(objType), key, data, timestamp);
	}

	/**
	 *
	 * @param rowKey - Row key of the index_update row
	 * @param id - Specific id of the item in the row to delete
	 * @return Single CQLStatement that runs the delete
	 */
	@NotNull
	public CQLStatement makeCQLforDeleteObsoleteUpdateIndexColumn(IndexUpdateRowKey rowKey, UUID id){
		return CQLStatement.make(
				String.format(TEMPLATE_DELETE_OBSOLETE_UPDATE_INDEX_COLUMN, this.keyspace),
				Arrays.asList(rowKey.getObjectName(), rowKey.getInstanceId(), id).toArray());
	}

	/**
	 *
	 * @return String of single CQL statement required to create the Shard Index Table
	 */
	public static CQLStatement makeCQLforShardIndexTableCreate(){
		return CQLStatement.make(String.format(TEMPLATE_CREATE_WIDE_INDEX,CObjectShardList.SHARD_INDEX_TABLE_NAME));
	}

	/**
	 *
	 * @return String of single CQL statement required to create the Shard Index Table
	 */
	public static CQLStatement makeCQLforShardIndexTableDrop(){
		return CQLStatement.make(String.format(TEMPLATE_DROP, CObjectShardList.SHARD_INDEX_TABLE_NAME));
	}

	/**
	 *
	 * @return CQLStatement of single CQL statement required to get the first update token
	 */
	public CQLStatement makeGetFirstEligibleIndexUpdate(){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_FIRST_ELIGIBLE_INDEX_UPDATE, keyspace), Arrays.asList(getTimeUUIDAtEndOfConsistencyHorizion()).toArray());
	}

	/**
	 *
	 * @param lastInstanceKey - Row Key representing the position of the previous row key
	 * @return CQLStatement of the single CQL statement required to get the next update token
	 */
	public CQLStatement makeGetNextEligibleIndexUpdate(IndexUpdateRowKey lastInstanceKey){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_NEXT_ELIGIBLE_INDEX_UPDATE, keyspace), Arrays.asList(lastInstanceKey.getObjectName(),lastInstanceKey.getInstanceId(),getTimeUUIDAtEndOfConsistencyHorizion()).toArray());
	}

	/**
	 *
	 * @param instanceKey - Row Key representing the row key for the row to retrieve
	 * @return CQLStatement of the single CQL statement required to get the Row corresponding to the token
	 */
	public static CQLStatement makeGetRowIndexUpdate(String keyspace, IndexUpdateRowKey instanceKey){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_ROW_INDEX_UPDATE, keyspace), Arrays.asList(instanceKey.getObjectName(),instanceKey.getInstanceId()).toArray());
	}

	/**
	 *
	 * @param def - CIndex for the index for which to pull the shard list
	 * @param indexValues - Values identifing the specific index for which to pull the shard list
	 * @param ordering - ASC or DESC
	 * @param start - Start UUID for bounding
	 * @param end - End UUID for bounding
	 * @return Single CQL statement needed to retrieve the list of shardids
	 */
	public static CQLStatement makeCQLforGetShardIndexList(String keyspace, CDefinition def, SortedMap<String,Object> indexValues, CObjectOrdering ordering,@Nullable UUID start, @Nullable UUID end) throws CQLGenerationException {
		CIndex i = def.getIndex(indexValues);
		String indexValueString = makeIndexValuesString(indexValues.values());
		List values = Lists.newArrayList();
		values.add(makeTableName(def,i));
		values.add(indexValueString);

		String whereCQL = "";
		if(start != null){
			whereCQL += " AND shardid >= ?";
			values.add(Long.valueOf(i.getShardingStrategy().getShardKey(start)));
		}
		if(end != null){
			whereCQL += " AND shardid <= ?";
			values.add(Long.valueOf(i.getShardingStrategy().getShardKey(end)));
		}
		String query =  String.format(
			TEMPLATE_SELECT_WIDE_INDEX,
            keyspace,
			CObjectShardList.SHARD_INDEX_TABLE_NAME,
			whereCQL,
			ordering
		);
		logger.debug("Making statement with query: {} and values: {}", query, values);
		return CQLStatement.make(query, values.toArray());
	}

    private CQLStatementIterator makeCQLforLeveledCompaction(CKeyspaceDefinition keyspaceDefinition, Integer sstableSize){
        List ret =  Lists.newArrayList();
        //global tables
        ret.add(makeCQLforLeveledCompaction(keyspaceDefinition.getName(), "__shardindex", sstableSize));
        ret.add(makeCQLforLeveledCompaction(keyspaceDefinition.getName(), "__index_updates", sstableSize));

        //CDefinition tables
        for(CDefinition def : keyspaceDefinition.getDefinitions().values()){
            //static table
            ret.add(makeCQLforLeveledCompaction(keyspaceDefinition.getName(), makeTableName(def, null), sstableSize));
            //indexes
            for(CIndex index : def.getIndexes().values()){
                ret.add(makeCQLforLeveledCompaction(keyspaceDefinition.getName(), makeTableName(def,index), sstableSize));
            }
        }
        return new BoundedCQLStatementIterator(ret);
    }

    private CQLStatementIterator makeCQLforTieredCompaction(CKeyspaceDefinition keyspaceDefinition, Integer minThreshold){
        List ret =  Lists.newArrayList();
        //global tables
        ret.add(makeCQLforTieredCompaction(keyspaceDefinition.getName(), "__shardindex", minThreshold));
        ret.add(makeCQLforTieredCompaction(keyspaceDefinition.getName(), "__index_updates", minThreshold));

        //CDefinition tables
        for(CDefinition def : keyspaceDefinition.getDefinitions().values()){
            //static table
            ret.add(makeCQLforTieredCompaction(keyspaceDefinition.getName(), makeTableName(def, null), minThreshold));
            //indexes
            for(CIndex index : def.getIndexes().values()){
                ret.add(makeCQLforTieredCompaction(keyspaceDefinition.getName(), makeTableName(def,index), minThreshold));
            }
        }
        return new BoundedCQLStatementIterator(ret);
    }

    public CQLStatementIterator makeCQLforCompaction(CKeyspaceDefinition keyspaceDefinition, String strategy, Map<String,Object> options) throws CQLGenerationException {
        if(strategy.equals("LeveledCompactionStrategy")){
            Integer sstableSize = (options.get("sstable_size_in_mb") == null) ? 5 : (Integer)options.get("sstable_size_in_mb");
            return makeCQLforLeveledCompaction(keyspaceDefinition,sstableSize);
        }
        else if(strategy.equals("SizeTieredCompactionStrategy")){
            Integer minThreshold = (options.get("min_threshold") == null) ? 6 : (Integer)options.get("min_threshold");
            return makeCQLforTieredCompaction(keyspaceDefinition,minThreshold);
        }
        throw new CQLGenerationException("Unknown Strategy " + strategy);
    }

    /**
     * @param table - The table to update with the compaction strategy
     * @param sstableSize - the size in MB of the ss tables
     * @return String of single CQL statement required to set
     */
    public static CQLStatement makeCQLforLeveledCompaction(String keyspace, String table, Integer sstableSize){
        return CQLStatement.make(String.format(TEMPLATE_SET_COMPACTION_LEVELED, keyspace, table, sstableSize));
    }

    /**
     * @param table - The table to update with the compaction strategy
     * @param minThreshold - minimum number of SSTables to trigger a minor compaction
     * @return String of single CQL statement required to set
     */
    public static CQLStatement makeCQLforTieredCompaction(String keyspace, String table, Integer minThreshold){
        return CQLStatement.make(String.format(TEMPLATE_SET_COMPACTION_TIERED, keyspace, table, minThreshold));
    }

	public static CQLStatement makeCQLforIndexUpdateTableCreate(){
		return CQLStatement.make(TEMPLATE_CREATE_INDEX_UPDATES);
	}

	public static CQLStatementIterator makeCQLforUpdate(String keyspace, CDefinition def, UUID key, Map<String,Object> oldValues, Map<String, Object> newValues) throws CQLGenerationException {
		List<CQLStatement> ret = Lists.newArrayList();
		//(1) Detect if there are any changed index values in values
		List<CIndex> effectedIndexes = getEffectedIndexes(def, oldValues, newValues);
		List<CIndex> uneffectedIndexes = getUneffectedIndexes(def, oldValues, newValues);

		//(2) Delete from any indexes that are no longer applicable
		for(CIndex i : effectedIndexes){
			Map<String,Object> compositeKeyToDelete = i.getIndexKeyAndValues(oldValues);
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to delete on this index. If not just continue
				// because it would be ignored on insert
				if(!i.validateIndexKeys(compositeKeyToDelete)){
					continue;
				}
			}
			ret.add(makeCQLforDeleteUUIDFromIndex(keyspace, def, i, key, compositeKeyToDelete, null));
		}

		//(3) Construct a complete copy of the object
		Map<String,Object> completeValues = Maps.newHashMap(oldValues);
		for(String k : newValues.keySet()){
			completeValues.put(k, newValues.get(k));
		}
		Map<String,ArrayList> fieldsAndValues = makeFieldAndValueList(def,completeValues);

		//(4) Add index values to the new values list
		Map<String,Object> newValuesAndIndexValues = Maps.newHashMap(newValues);
		for(String s: def.getRequiredFields()){
			if(!newValuesAndIndexValues.containsKey(s)){
				newValuesAndIndexValues.put(s, completeValues.get(s));
			}
		}
		Map<String,ArrayList> fieldsAndValuesForNewValuesAndIndexValues = makeFieldAndValueList(def,newValuesAndIndexValues);

		//(5) Insert into the new indexes like a new insert
		for(CIndex i: effectedIndexes){
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to insert on this index. If not just continue;
				if(!i.validateIndexKeys(i.getIndexKeyAndValues(completeValues))){
					continue;
				}
			}
			addCQLStatmentsForIndexInsert(keyspace, true, ret, def, completeValues, i, key, fieldsAndValues,null, null);
		}

		//(6) Insert into the existing indexes without the shard index addition
		for(CIndex i: uneffectedIndexes){
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to insert on this index. If not just continue;
				if(!i.validateIndexKeys(i.getIndexKeyAndValues(newValuesAndIndexValues))){
					continue;
				}
			}
			addCQLStatmentsForIndexInsert(keyspace, false, ret, def, newValuesAndIndexValues, i, key, fieldsAndValuesForNewValuesAndIndexValues,null, null);
		}

		//(7) Update the static table (be sure to only update and not insert the completevalues just in case they are wrong, the background job will fix them later)
		Map<String,ArrayList> fieldsAndValuesOnlyForChanges = makeFieldAndValueList(def,newValues);
		ret.add(makeInsertStatementStatic(
				keyspace,
                makeTableName(def,null),
				(List<String>)fieldsAndValuesOnlyForChanges.get("fields").clone(),
				(List<Object>)fieldsAndValuesOnlyForChanges.get("values").clone(),
				key,
				null,
				null
		));

		//(8) Insert a snapshot of the updated values for this id into the __index_updates
		ret.add(makeInsertUpdateIndexStatement(keyspace, def, key, def.makeIndexValues(completeValues)));

		return new BoundedCQLStatementIterator(ret);
	}

	public static List<CIndex> getEffectedIndexes(CDefinition def, Map<String,Object> oldValues, Map<String,Object> newValues){
		List<CIndex> ret = Lists.newArrayList();
		if(def.getIndexes() == null) {
			return ret;
		}
		for(CIndex i : def.getIndexes().values()){
			if(i.areValuesAssociatedWithIndex(newValues)){
				//This change does indeed effect this index
				ret.add(i);
			}
		}
		return ret;
	}

	public static List<CIndex> getUneffectedIndexes(CDefinition def, Map<String,Object> oldValues, Map<String,Object> newValues){
		List<CIndex> ret = Lists.newArrayList();
		if(def.getIndexes() == null) {
			return ret;
		}
		for(CIndex i : def.getIndexes().values()){
			if(!i.areValuesAssociatedWithIndex(newValues)){
				//This change does not effect this index
				ret.add(i);
			}
		}
		return ret;
	}

	public static CQLStatementIterator makeCQLforCreate(CDefinition def){
		List<CQLStatement> ret = Lists.newArrayList();
		ret.add(makeStaticTableCreate(def));
		if(def.getIndexes() != null) {
			for(CIndex i : def.getIndexes().values()){
				ret.add(makeWideTableCreate(def, i));
			}
		}
		return new BoundedCQLStatementIterator(ret);
	}


	protected static CQLStatementIterator makeCQLforDrop(String keyspace, CDefinition def){
		List<CQLStatement> ret = Lists.newArrayList();
		ret.add(makeTableDrop(keyspace, def.getName()));
		if(def.getIndexes() != null) {
			for(CIndex i : def.getIndexes().values()){
				ret.add(makeTableDrop(keyspace, makeTableName(def, i)));
			}
		}
		return new BoundedCQLStatementIterator(ret);
	}


	protected static CQLStatement makeInsertStatementStatic(String keyspace, String tableName, List<String> fields, List values, UUID uuid, Long timestamp, Integer ttl){
		fields.add(0,"id");
		values.add(0, uuid);
		String query = String.format(
				TEMPLATE_INSERT_STATIC,
                keyspace,
				tableName,
				makeCommaList(fields),
				makeCommaList(values, true),
				//timestamp.toString(), //add timestamp back when timestamps become preparable
				(ttl == null) ? "" : (" USING TTL "+ttl)//(" AND TTL "+ttl) //Revert this back to AND when timestamps are preparable
		);

		return CQLStatement.make(query, values.toArray());
	}

	public UUID getTimeUUIDAtEndOfConsistencyHorizion(){
		UUID ret = UUIDs.startOf(DateTime.now().getMillis() - consistencyHorizon);//now minus 5 seconds
		return ret;
	}

	public static CQLStatement makeInsertUpdateIndexStatement(String keyspace, CDefinition def, UUID instanceId, Map<String,Object> indexvalues) throws CQLGenerationException {
		UUID id = UUIDs.timeBased();
		String tableName = makeTableName(def,null);
		String indexValuesAsJson;
		try{
			ObjectMapper om = new ObjectMapper();
			indexValuesAsJson = om.writeValueAsString(indexvalues);
		}
		catch (Exception e){
			throw new CQLGenerationException(e.getMessage());
		}
		return CQLStatement.make(String.format(TEMPLATE_INSERT_INDEX_UPDATES,keyspace), Arrays.asList(id, tableName, instanceId, indexValuesAsJson).toArray() );
	}

	protected static CQLStatement makeInsertStatementWide(String keyspace, String tableName, List<String> fields, List<Object> values, UUID uuid, long shardid, Long timestamp, Integer ttl){
		fields.add(0,"shardid");
		values.add(0,Long.valueOf(shardid));
		fields.add(0,"id");
		values.add(0,uuid);

		String query = String.format(
			TEMPLATE_INSERT_WIDE,
            keyspace,
			tableName,
			makeCommaList(fields),
			makeCommaList(values,true),
			//timestamp.toString(), //add timestamp back when timestamps become preparable
			(ttl == null) ? "" : (" USING TTL "+ttl)//(" AND TTL "+ttl) //Revert this back to AND when timestamps are preparable
		);

		return CQLStatement.make(query, values.toArray());
	}

	protected static CQLStatement makeInsertStatementWideIndex(String keyspace, String tableName, String targetTableName, long shardId, List indexValues, Long timestamp) throws CQLGenerationException {
		String indexValuesString = makeIndexValuesString(indexValues);
		Object[] values = {targetTableName, indexValuesString, Long.valueOf(shardId), shardId+":"+indexValuesString};
		return CQLStatement.make(String.format(
				TEMPLATE_INSERT_WIDE_INDEX,
                keyspace,
				tableName
				//timestamp.toString() //Add back timestamp when timestamps become preparable
			),values);
	}

	public static CQLStatementIterator makeCQLforInsertKeyspaceDefinition(@NotNull String keyspace, @NotNull String keyspaceDefinition, @NotNull UUID id) throws CQLGenerationException{
		ArrayList<CQLStatement> ret = Lists.newArrayList();
		ret.add(CQLStatement.make(String.format(TEMPLATE_INSERT_KEYSPACE, keyspace), Arrays.asList(id, Long.valueOf(1), keyspaceDefinition).toArray()));
		return new BoundedCQLStatementIterator(ret);
	}

	protected static CQLStatementIterator makeCQLforInsert(@NotNull String keyspace, @NotNull CDefinition def, @NotNull Map<String,Object> data) throws CQLGenerationException{
		return makeCQLforInsert(keyspace, def, data, null, null, 0);
	}

	protected static CQLStatementIterator makeCQLforInsert(@NotNull String keyspace, @NotNull CDefinition def, @NotNull Map<String,Object> data, @Nullable UUID uuid, Long timestamp, Integer ttl) throws CQLGenerationException{
		List<CQLStatement> ret = Lists.newArrayList();
		if(uuid == null){
			uuid = UUIDs.timeBased();
		}
		if(timestamp == 0){
			timestamp = System.currentTimeMillis();
		}
		if(!validateData(def, data)){
			throw new CQLGenerationException("Invalid Insert Requested. Missing Field(s)");
		}
		Map<String,ArrayList> fieldsAndValues = makeFieldAndValueList(def,data);
		//Static Table
		ret.add(makeInsertStatementStatic(
                keyspace,
				makeTableName(def,null),
				(List<String>)fieldsAndValues.get("fields").clone(),
				(List<Object>)fieldsAndValues.get("values").clone(),
				uuid,
				timestamp,
				ttl
		));
		//Index Tables
		if(def.getIndexes() != null) {
			for(CIndex i : def.getIndexes().values()){
				if(def.isAllowNullPrimaryKeyInserts()){
					//check if we have the necessary primary fields to insert on this index. If not just continue;
					if(!i.validateIndexKeys(i.getIndexKeyAndValues(data))){
						continue;
					}
				}
				//insert it into the index
				addCQLStatmentsForIndexInsert(keyspace, true, ret, def,data,i,uuid,fieldsAndValues,timestamp,ttl);
			}
		}
		return new BoundedCQLStatementIterator(ret);
	}

	public static void addCQLStatmentsForIndexInsert(String keyspace, boolean includeShardInsert, List<CQLStatement> statementListToAddTo, CDefinition def, @NotNull Map<String,Object> data, CIndex i, UUID uuid, Map<String,ArrayList> fieldsAndValues,Long timestamp, Integer ttl) throws CQLGenerationException {
		//insert it into the index
		long shardId = i.getShardingStrategy().getShardKey(uuid);
		statementListToAddTo.add(makeInsertStatementWide(
                keyspace,
				makeTableName(def,i),
				(List<String>)fieldsAndValues.get("fields").clone(),
				(List<Object>)fieldsAndValues.get("values").clone(),
				uuid,
				shardId,
				timestamp,
				ttl
		));
		if( includeShardInsert && (!(i.getShardingStrategy() instanceof ShardingStrategyNone))){
			//record that we have made an insert into that shard
			statementListToAddTo.add(makeInsertStatementWideIndex(
                    keyspace,
					CObjectShardList.SHARD_INDEX_TABLE_NAME,
					makeTableName(def,i),
					shardId,
					i.getIndexValues(data),
					timestamp
			));
		}
	}

	protected static CQLStatementIterator makeCQLforGet(String keyspace, CDefinition def, UUID key){
		Object[] values = {key};
		CQLStatement statement = CQLStatement.make(String.format(TEMPLATE_SELECT_STATIC, keyspace, def.getName(),"id = ?"), values);
		return new BoundedCQLStatementIterator(Lists.newArrayList(statement));
	}

	protected static CQLStatement makeCQLforTableScan(String keyspace, CDefinition def){
		return CQLStatement.make(String.format(TEMPLATE_TABLE_SCAN, keyspace, def.getName()));
	}

	public static CQLStatement makeCQLforGetKeyspaceDefinitions(String keyspace){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_KEYSPACES, keyspace));
	}

	@NotNull
	protected static CQLStatementIterator makeCQLforGet(String keyspace, CObjectShardList shardList, CDefinition def, SortedMap<String,Object> indexValues, CObjectOrdering ordering,@Nullable UUID start, @Nullable UUID end, Long limit, boolean inclusive) throws CQLGenerationException {
		return makeCQLforGet(keyspace, shardList, def, indexValues, ordering, start, end, limit, inclusive, false);
	}

		@NotNull
	protected static CQLStatementIterator makeCQLforGet(String keyspace, CObjectShardList shardList, CDefinition def, SortedMap<String,Object> indexValues, CObjectOrdering ordering,@Nullable UUID start, @Nullable UUID end, Long limit, boolean inclusive, boolean countOnly) throws CQLGenerationException {

		CIndex i = def.getIndex(indexValues);
		if(i == null){
			throw new CQLGenerationException(String.format("Could not find specified index on CDefinition %s",def.getName()));
		}
		if(!i.validateIndexKeys(indexValues)){
			throw new CQLGenerationException(String.format("Cannot query index %s on CDefinition %s with the provided list of index values",i.getName(),def.getName()));
		}
		CQLStatement whereCQL = makeAndedEqualList(def,indexValues);
		String whereQuery = whereCQL.getQuery();
		List values = new ArrayList(Arrays.asList(whereCQL.getValues()));
		if(start != null){
			whereQuery +=  " AND id >"+(inclusive ? "= "  :  " ")+ "?";
			values.add(start);
		}
		if(end != null){
			whereQuery += " AND id <"+(inclusive ? "= "  :  " ") + "?";
			values.add(end);
		}
		String limitCQL = "";
		if(limit.longValue() > 0){
			limitCQL = "LIMIT %d";
		}
		String CQLTemplate = String.format(
			TEMPLATE_SELECT_WIDE,
			countOnly ? "count(*)":"*",
			keyspace,
			makeTableName(def,i),
			"?",
			whereQuery,
			ordering,
			limitCQL);

		CQLStatement templateCQLStatement = CQLStatement.make(CQLTemplate, values.toArray());
		templateCQLStatement.setCacheable(true);

		Long starttime = (start == null) ? null : Long.valueOf(UUIDs.unixTimestamp(start));
		Long endtime = (end == null) ? null : Long.valueOf(UUIDs.unixTimestamp(end));
		if( (starttime != null && endtime != null) || (i.getShardingStrategy() instanceof ShardingStrategyNone) ){
			//the query is either bounded or unsharded, so we do not need to check the shardindex
			try{
				Range<Long> shardIdRange = i.getShardingStrategy().getShardKeyRange(starttime,endtime);
				return new UnboundableCQLStatementIterator(shardIdRange,limit,ordering,templateCQLStatement);
			}
			catch(ShardStrategyException e){
				throw new CQLGenerationException(e.getMessage());
			}
		}
		else{
			//we have an unbounded query
			return new BoundedLazyCQLStatementIterator(
					shardList.getShardIdList(def,indexValues,ordering,start,end),
					templateCQLStatement,
					limit
			);
		}
	}

	protected static CQLStatementIterator makeCQLforGet(String keyspace, CObjectShardList shardList, CDefinition def, SortedMap<String,Object> indexvalues, Long limit) throws CQLGenerationException {
		DateTime now = new DateTime(DateTimeZone.UTC);
		long unixtimestamp = (long)now.getMillis();
		return makeCQLforGet(keyspace, shardList, def, indexvalues, CObjectOrdering.DESCENDING, null, UUIDs.endOf(unixtimestamp), limit, false);
	}

	protected static CQLStatementIterator makeCQLforGet(String keyspace, CObjectShardList shardList, CDefinition def, SortedMap<String,Object> indexvalues, CObjectOrdering ordering,Long starttimestamp, Long endtimestamp, Long limit) throws CQLGenerationException {
		UUID startUUID = (starttimestamp == null) ? null : UUIDs.startOf(starttimestamp.longValue());
		UUID endUUID = (endtimestamp == null) ? null : UUIDs.endOf(endtimestamp.longValue());
		return makeCQLforGet(keyspace, shardList, def,indexvalues,ordering,startUUID,endUUID,limit, true);
	}

	protected static CQLStatementIterator makeCQLforDelete(String keyspace, CDefinition def, UUID key, Map<String,Object> data, Long timestamp){
		if(timestamp == null){
			timestamp = Long.valueOf(System.currentTimeMillis());
		}
		List<CQLStatement> ret = Lists.newArrayList();
		ret.add(makeCQLforDeleteUUIDFromStaticTable(keyspace, def, key, timestamp));
		for(CIndex i : def.getIndexes().values()){
			ret.add(makeCQLforDeleteUUIDFromIndex(keyspace, def, i, key, i.getIndexKeyAndValues(data), timestamp));
		}
		return new BoundedCQLStatementIterator(ret);
	}

	protected static CQLStatement makeCQLforDeleteUUIDFromStaticTable(String keyspace, CDefinition def, UUID uuid, Long timestamp){
		Object[] values = {uuid};
		return CQLStatement.make(String.format(
				TEMPLATE_DELETE,
				keyspace,
				makeTableName(def, null),
				//timestamp, //Add back when timestamps become preparable
				"id = ?")
			, values);
	}


	public static CQLStatement makeCQLforDeleteUUIDFromIndex(String keyspace, CDefinition def, CIndex index, UUID uuid, Map<String,Object> indexValues, Long timestamp){
		List values = Lists.newArrayList( uuid, Long.valueOf(index.getShardingStrategy().getShardKey(uuid)) );
		CQLStatement wheres = makeAndedEqualList(def, indexValues);
		values.addAll(Arrays.asList(wheres.getValues()));
		String whereCQL = String.format( "id = ? AND shardid = ? AND %s", wheres.getQuery());
		String query = String.format(
			TEMPLATE_DELETE,
			keyspace,
			makeTableName(def,index),
			//timestamp, //Add back when timestamps become preparable
			whereCQL);
		return CQLStatement.make(query,values.toArray());
	}

	public static Statement makeCQLforDeleteUUIDFromIndex_WorkaroundForUnpreparableTimestamp(String keyspace, CDefinition def, CIndex index, UUID uuid, Map<String,Object> indexValues, Long timestamp){
		Statement ret = QueryBuilder.delete()
						.from(keyspace,makeIndexTableName(def,index))
						.using(QueryBuilder.timestamp(timestamp))
						.where(QueryBuilder.eq("id",uuid))
						.and(QueryBuilder.eq("shardid", Long.valueOf(index.getShardingStrategy().getShardKey(uuid))));
		for(String key : indexValues.keySet()){
			((Delete.Where)ret).and(QueryBuilder.eq(key,indexValues.get(key)));
		}
		return ret;
	}

	protected static CQLStatement makeTableDrop(String keyspace, String tableName){
		return CQLStatement.make(String.format(TEMPLATE_DROP, keyspace, tableName));
	}

	protected static CQLStatement makeStaticTableCreate(CDefinition def){
		String query = String.format(
			TEMPLATE_CREATE_STATIC,
			def.getName(),
			makeFieldList(def.getFields().values(),true));
		return CQLStatement.make(query);
	}

	public static CQLStatement makeWideTableCreate(CDefinition def, CIndex index){
		String query = String.format(
			TEMPLATE_CREATE_WIDE,
			makeTableName(def,index),
			makeFieldList(def.getFields().values(), true),
			makeCommaList(index.getCompositeKeyList()));
		return CQLStatement.make(query);
	}

	public static String makeIndexValuesString(Collection values) throws CQLGenerationException{
		//note, this escaping mechanism can in very rare situations cause index collisions, for example
		//one:two as a value collides with another value one&#58;two
		List<String> escaped = Lists.newArrayList();
		for(Object v : values){
			escaped.add(coerceValueToString(v).replaceAll(":", "&#58;"));
		}
		return Joiner.on(":").join(escaped);
	}

	public static String coerceValueToString(Object value) throws CQLGenerationException {
		if(value instanceof String){
			return (String)value;
		}
		if( (value instanceof UUID) || (value instanceof Long) || (value instanceof Boolean) || (value instanceof Float) || (value instanceof Double) || (value instanceof Integer) || (value instanceof BigInteger) ){
			return value.toString();
		}
		if( value instanceof java.util.Date){
			return ((java.util.Date)value).getTime()+"";
		}
		throw new CQLGenerationException("Rhombus does not support indexes on fields of type " + value.getClass().toString());
	}

	public static Map<String,ArrayList> makeFieldAndValueList(CDefinition def, Map<String,Object> data) throws CQLGenerationException{
		ArrayList fieldList = Lists.newArrayList();
		ArrayList valueList = Lists.newArrayList();
		for(CField f : def.getFields().values()){
			if( data.containsKey(f.getName()) ){
				fieldList.add(f.getName());
				valueList.add(data.get(f.getName()));
			}
		}
		Map<String,ArrayList> ret = Maps.newHashMap();
		ret.put("fields", fieldList);
		ret.put("values", valueList);
		return ret;
	}

	protected static boolean validateData(CDefinition def, Map<String,Object> data){
		if(def.isAllowNullPrimaryKeyInserts()){
			return true;
		}
		Collection<String> fields = def.getRequiredFields();
		for( String f : fields){
			if(!data.containsKey(f)){
				return false;
			}
		}
		return true;
	}

	protected static CQLStatement makeAndedEqualList(CDefinition def, Map<String,Object> data){
		String query = "";
		List values = Lists.newArrayList();
		int count = 0;
		for(String key : data.keySet()){
			CField f = def.getFields().get(key);
			query+=f.getName() + " = ?";
			values.add(data.get(key));
			if(++count < data.keySet().size()){
				query += " AND ";
			}
		}
		return CQLStatement.make(query, values.toArray());
	}

	protected static String makeCommaList(List strings, boolean onlyQuestionMarks){
		Iterator<Object> it = strings.iterator();
		String ret = "";
		while(it.hasNext()){
			Object thenext = it.next();
			String thenextstring = thenext == null ? "null" : thenext.toString();
			String s = onlyQuestionMarks ? "?" : thenextstring;
			ret = ret + s +(it.hasNext() ? ", " : "");
		}
		return ret;
	}

	protected static String makeCommaList(List strings){
		return makeCommaList(strings, false);
	}

	protected static String makeFieldList(Collection<CField> fields, boolean withType){
		Iterator<CField> it = fields.iterator();
		String ret = "";
		while(it.hasNext()){
			CField f = it.next();
			ret = ret + f.getName() +
					(withType ? " " + f.getType() : "") +
					(it.hasNext() ? "," : "");
		}
		return ret;
	}

	protected static String makeTableName(CDefinition def, @Nullable CIndex index){
		String objName = def.getName();
		if(index == null){
			return objName;
		}
		else{
			return makeIndexTableName(def,index);
		}
	}

	protected static String makeIndexTableName(CDefinition def, CIndex index){
		String indexName = Joiner.on('_').join(index.getCompositeKeyList());
		String hash = DigestUtils.md5Hex(def.getName()+"|"+indexName);
		//md5 hashes (in hex) give us 32 chars. We have 48 chars available so that gives us 16 chars remaining for a pretty
		//display name for the object type.
		String objDisplayName = def.getName().length() > 15 ? def.getName().substring(0,16) : def.getName();
		return objDisplayName+hash;
	}

	public void setShardList(CObjectShardList shardList) {
		this.shardList = shardList;
	}

}