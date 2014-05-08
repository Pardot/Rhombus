package com.pardot.rhombus.cobject;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.cobject.shardingstrategy.ShardStrategyException;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import com.pardot.rhombus.cobject.statement.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
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

	protected static final String KEYSPACE_DEFINITIONS_TABLE_NAME = "__keyspace_definitions";
	protected static final String INDEX_UPDATES_TABLE_NAME = "__index_updates";
	protected static final long CQL_LIMIT_MAX = 10000;

	protected static final String TEMPLATE_CREATE_STATIC = "CREATE TABLE \"%s\".\"%s\" (id %s PRIMARY KEY, %s);";
	protected static final String TEMPLATE_CREATE_WIDE = "CREATE TABLE \"%s\".\"%s\" (id %s, shardid bigint, %s, PRIMARY KEY ((shardid, %s),id) );";
	protected static final String TEMPLATE_CREATE_KEYSPACE_LIST = "CREATE TABLE \"%s\".\"" + KEYSPACE_DEFINITIONS_TABLE_NAME + "\" (id uuid, name varchar, def varchar, PRIMARY KEY ((name), id));";
	protected static final String TEMPLATE_CREATE_WIDE_INDEX = "CREATE TABLE \"%s\".\"%s\" (shardid bigint, tablename varchar, indexvalues varchar, targetrowkey varchar, PRIMARY KEY ((tablename, indexvalues),shardid) );";
	protected static final String TEMPLATE_CREATE_INDEX_UPDATES = "CREATE TABLE \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" (id timeuuid, statictablename varchar, instanceid timeuuid, indexvalues varchar, PRIMARY KEY ((statictablename,instanceid),id))";
	protected static final String TEMPLATE_TRUNCATE_INDEX_UPDATES = "TRUNCATE \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\";";
	protected static final String TEMPLATE_DROP = "DROP TABLE \"%s\".\"%s\";";
	protected static final String TEMPLATE_TRUNCATE = "TRUNCATE \"%s\".\"%s\";";
	protected static final String TEMPLATE_INSERT_STATIC = "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)%s;";//"USING TIMESTAMP %s%s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_WIDE = "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)%s;";//"USING TIMESTAMP %s%s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_KEYSPACE = "INSERT INTO \"%s\".\"" + KEYSPACE_DEFINITIONS_TABLE_NAME + "\" (id, name, def) values (?, ?, ?);";
	protected static final String TEMPLATE_INSERT_WIDE_INDEX = "INSERT INTO \"%s\".\"%s\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);";//"USING TIMESTAMP %s;";//Add back when timestamps become preparable
	protected static final String TEMPLATE_INSERT_INDEX_UPDATES = "INSERT INTO \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" (id, statictablename, instanceid, indexvalues) values (?, ?, ?, ?);";
	protected static final String TEMPLATE_SELECT_STATIC = "SELECT * FROM \"%s\".\"%s\" WHERE %s;";
	protected static final String TEMPLATE_SELECT_WIDE = "SELECT %s FROM \"%s\".\"%s\" WHERE shardid = %s AND %s ORDER BY id %s %s ALLOW FILTERING;";
	protected static final String TEMPLATE_SELECT_KEYSPACE = "SELECT def FROM \"%s\".\"" + KEYSPACE_DEFINITIONS_TABLE_NAME + "\" WHERE name = ? ORDER BY id DESC LIMIT 1;";
	protected static final String TEMPLATE_SELECT_WIDE_INDEX = "SELECT shardid FROM \"%s\".\"%s\" WHERE tablename = ? AND indexvalues = ?%s ORDER BY shardid %s ALLOW FILTERING;";
	protected static final String TEMPLATE_DELETE = "DELETE FROM \"%s\".\"%s\" WHERE %s;";//"DELETE FROM %s USING TIMESTAMP %s WHERE %s;"; //Add back when timestamps become preparable
	protected static final String TEMPLATE_DELETE_OBSOLETE_UPDATE_INDEX_COLUMN = "DELETE FROM \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" WHERE  statictablename = ? and instanceid = ? and id = ?";
	protected static final String TEMPLATE_SELECT_FIRST_ELIGIBLE_INDEX_UPDATE = "SELECT statictablename,instanceid FROM \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" WHERE id < ? limit 1 allow filtering;";
	protected static final String TEMPLATE_SELECT_NEXT_ELIGIBLE_INDEX_UPDATE = "SELECT statictablename,instanceid FROM \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" where token(statictablename,instanceid) > token(?,?) and id < ? limit 1 allow filtering;";
	protected static final String TEMPLATE_SELECT_ROW_INDEX_UPDATE = "SELECT * FROM \"%s\".\"" + INDEX_UPDATES_TABLE_NAME + "\" where statictablename = ? and instanceid = ? order by id DESC;";
	protected static final String TEMPLATE_SET_COMPACTION_LEVELED = "ALTER TABLE \"%s\".\"%s\" WITH compaction = { 'class' :  'LeveledCompactionStrategy',  'sstable_size_in_mb' : %d }";
	protected static final String TEMPLATE_SET_COMPACTION_TIERED = "ALTER TABLE \"%s\".\"%s\" WITH compaction = { 'class' :  'SizeTieredCompactionStrategy',  'min_threshold' : %d }";
	protected static final String TEMPLATE_TABLE_SCAN = "SELECT * FROM \"%s\".\"%s\";";
	protected static final String TEMPLATE_ADD_FIELD = "ALTER TABLE \"%s\".\"%s\" add %s %s";

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
		return CQLStatement.make(String.format(TEMPLATE_CREATE_KEYSPACE_LIST, keyspace), KEYSPACE_DEFINITIONS_TABLE_NAME);
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
	 * @return Iterator of CQL statements that need to be executed for this task.
	 */
	public CQLStatementIterator makeCQLforTruncate(String objType){
		return makeCQLforTruncate(this.keyspace, this.definitions.get(objType));
	}

    /**
     *
     * @param objType - The name of the Object type aka CDefinition.name
     * @return Iterator of CQL statements that need to be executed for this task.
     * @throws CQLGenerationException
     */
    @NotNull
    public CQLStatement makeCQLforInsertNoValuesforStaticTable(String objType) throws CQLGenerationException {
        CDefinition definition = this.definitions.get(objType);
        Map<String, CField> fields = Maps.newHashMap(definition.getFields());
        Object id = null;
        if (fields.containsKey("id")) {
            id = fields.get("id");
            fields.remove("id");
        }
        List<String> fieldNames = new ArrayList<String>(fields.keySet());
        List<String> valuePlaceholders = new ArrayList<String>(fields.keySet());
        return makeInsertStatementStatic(this.keyspace, definition.getName(), fieldNames, valuePlaceholders, id, null, null);
    }

    /**
     * @param definition The object definition of the wide table to insert into
     * @param tableName - The name of the wide table to insert into
     * @return CQL insert statement
     * @throws CQLGenerationException
     */
    @NotNull
    public CQLStatement makeCQLforInsertNoValuesforWideTable(CDefinition definition, String tableName, Long shardId) throws CQLGenerationException {
        Map<String, CField> fields = Maps.newHashMap(definition.getFields());
        Object id = null;
        if (fields.containsKey("id")) {
            id = fields.get("id");
            fields.remove("id");
        }
        List<String> fieldNames = new ArrayList<String>(fields.keySet());
        List<Object> valuePlaceholders = new ArrayList<Object>(fields.keySet());
        shardId = (shardId == null) ? 1L : shardId;
        return makeInsertStatementWide(this.keyspace, tableName, fieldNames, valuePlaceholders, id, shardId, null, null);
    }

    /**
     * @param tableName - The name of the wide table to insert into
     * @return CQL insert statement
     * @throws CQLGenerationException
     */
    @NotNull
    public CQLStatement makeCQLforInsertNoValuesforShardIndex(String tableName) throws CQLGenerationException {
        return CQLStatement.make(
                String.format(
                        TEMPLATE_INSERT_WIDE_INDEX,
                        keyspace,
                        tableName
                ),
                tableName,
                null);
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
	public CQLStatementIterator makeCQLforInsertKeyspaceDefinition(String name, String keyspaceDefinition) throws CQLGenerationException {
		return makeCQLforInsertKeyspaceDefinition(keyspace, name, keyspaceDefinition, UUIDs.timeBased());
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param data - A map of fieldnames to values representing the data to insert
	 * @return Iterator of CQL statements that need to be executed for this task.
	 * @throws CQLGenerationException
	 */
	@NotNull
	public CQLStatementIterator makeCQLforInsert(String objType, Map<String,Object> data, Object key, Long timestamp) throws CQLGenerationException {
		return makeCQLforInsert(this.keyspace, this.definitions.get(objType), data, key, timestamp, null);
	}

	/**
	 *
	 * @param objType - The name of the Object type aka CDefinition.name
	 * @param key - The TimeUUID of the object to retrieve
	 * @return Iterator of CQL statements that need to be executed for this task. (Should have a length of 1 for this particular method)
	 */
	@NotNull
	public CQLStatementIterator makeCQLforGet(String objType, Object key){
		return makeCQLforGet(this.keyspace, this.definitions.get(objType), key);
	}

	protected static CQLStatementIterator makeCQLforGet(String keyspace, CDefinition def, Object key){
		Object[] values = {key};
		CQLStatement statement = CQLStatement.make(String.format(TEMPLATE_SELECT_STATIC, keyspace, def.getName(), "id = ?"), def.getName(), values);
		return new BoundedCQLStatementIterator(Lists.newArrayList(statement));
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
	public CQLStatementIterator makeCQLforList(String objType, Criteria criteria, boolean countOnly) throws CQLGenerationException {
		CDefinition definition = this.definitions.get(objType);
		CObjectOrdering ordering = (criteria.getOrdering() != null ? criteria.getOrdering(): CObjectOrdering.DESCENDING);
		UUID endUuid = (criteria.getEndUuid() == null ? UUIDs.startOf(DateTime.now().getMillis()) : criteria.getEndUuid());
		return makeCQLforList(this.keyspace, shardList, definition, criteria.getIndexKeys(), ordering, criteria.getStartUuid(),
				endUuid, criteria.getLimit(), criteria.getInclusive(), countOnly, criteria.getAllowFiltering());
	}

	@NotNull
	protected static CQLStatementIterator makeCQLforList(String keyspace, CObjectShardList shardList, CDefinition def, SortedMap<String,Object> indexValues,
														 CObjectOrdering ordering, @Nullable UUID start, @Nullable UUID end, Long limit,
														 boolean inclusive, boolean countOnly, boolean allowFiltering) throws CQLGenerationException {
		// Get matching index from definition
		CIndex i = def.getIndex(indexValues, allowFiltering);
		if(i == null){
			throw new CQLGenerationException(String.format("Could not find specified index on CDefinition %s",def.getName()));
		}

		// Determine client filters and fix index values
		Map<String, Object> clientFilters = null;
		if(i.getCompositeKeyList().size() < indexValues.keySet().size()) {
			clientFilters = Maps.newHashMap();
			SortedMap<String, Object> newIndexValues = Maps.newTreeMap();
			for(String key : indexValues.keySet()) {
				if(i.getCompositeKeyList().contains(key)) {
					newIndexValues.put(key, indexValues.get(key));
				} else {
                    // Index keys will always exactly match the criteria index values if allowFiltering is false, so this only happens if allowFiltering is true
					clientFilters.put(key, indexValues.get(key));
				}
			}
			indexValues = newIndexValues;
		}

        boolean hasClientFilters = clientFilters != null && !clientFilters.isEmpty();

		// Now validate the remaining index values
		if(!i.validateIndexKeys(indexValues)){
			throw new CQLGenerationException(String.format("Cannot query index %s on CDefinition %s with the provided list of index values",i.getName(),def.getName()));
		}

		CQLStatement whereCQL = makeAndedEqualList(def,indexValues);
		String whereQuery = whereCQL.getQuery();
		List<Object> values = new ArrayList<Object>(Arrays.asList(whereCQL.getValues()));
		if(start != null){
			whereQuery += " AND id >" + (inclusive ? "= " : " ") + "?";
			values.add(start);
		}
		if(end != null){
			whereQuery += " AND id <" + (inclusive ? "= " : " ") + "?";
			values.add(end);
		}
		String limitCQL;

		// Do our best to come up with a sensible limit
		if(hasClientFilters) {
			if(limit * 2 >= 50l) {
				limit = limit * 2;
			} else {
				limit = 50l;
			}
		} else if(limit < 0) {
			limit = 50l;
		} else if(limit > CQL_LIMIT_MAX) {
			limit = CQL_LIMIT_MAX;
		}
		limitCQL = "LIMIT %d";


        // TODO: if we feel like it's worth the trouble, for count queries with client side filters, only select the fields needed to satisfy the filters
        // note that doing so will also require modifying ObjectMapper.mapResult() so it only maps fields that exist in the row
		String CQLTemplate = String.format(
				TEMPLATE_SELECT_WIDE,
                // If this was a count query and filtering was allowed and client filters weren't defined, just do a count query because we don't need to apply filters
                // Otherwise if this was a count query, but allowFiltering was true and we have client-side filters to apply, do a full row query so we can apply the filters
				countOnly && !(allowFiltering && hasClientFilters) ? "count(*)":"*",
				keyspace,
				makeTableName(def, i),
				"?",
				whereQuery,
				ordering,
				limitCQL);

		CQLStatement templateCQLStatement = CQLStatement.make(CQLTemplate, makeTableName(def, i), values.toArray());

		Long startTime = (start == null) ? null : UUIDs.unixTimestamp(start);
		Long endTime = (end == null) ? null : UUIDs.unixTimestamp(end);

		CQLStatementIterator returnIterator = null;
		if((startTime != null && endTime != null) || (i.getShardingStrategy() instanceof ShardingStrategyNone)) {
			//the query is either bounded or unsharded, so we do not need to check the shardindex
			try {
				Range<Long> shardIdRange = i.getShardingStrategy().getShardKeyRange(startTime,endTime);
				returnIterator = new UnboundableCQLStatementIterator(shardIdRange, limit, ordering, templateCQLStatement, def.getName());
			}
			catch(ShardStrategyException e){
				throw new CQLGenerationException(e.getMessage());
			}
		} else {
			//we have an unbounded query
			returnIterator = new BoundedLazyCQLStatementIterator(
					shardList.getShardIdList(def,indexValues,ordering,start,end),
					templateCQLStatement,
					limit,
					def.getName()
			);
		}

		// Set the client filters on the returned iterator so the client can take care of them
		returnIterator.setClientFilters(clientFilters);
		return returnIterator;
	}

	/**
	 *
	 * @return an iterator for getting all the keyspace definitions
	 */
	public CQLStatement makeCQLforGetKeyspaceDefinitions(String name){
		return makeCQLforGetKeyspaceDefinitions(this.keyspace, name);
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
				INDEX_UPDATES_TABLE_NAME,
				Arrays.asList(rowKey.getObjectName(), rowKey.getInstanceId(), id).toArray());
	}

	/**
	 *
	 * @return String of single CQL statement required to create the Shard Index Table
	 */
	public CQLStatement makeCQLforShardIndexTableCreate(){
		return CQLStatement.make(String.format(TEMPLATE_CREATE_WIDE_INDEX, this.keyspace, CObjectShardList.SHARD_INDEX_TABLE_NAME), CObjectShardList.SHARD_INDEX_TABLE_NAME);
	}

	private CQLStatement makeCQLforAddFieldToTable(String tableName, CField newField){
		String query = String.format(TEMPLATE_ADD_FIELD, this.keyspace, tableName, newField.getName(), newField.getType());
		return CQLStatement.make(query, tableName);
	}

	public CQLStatementIterator makeCQLforAddFieldToObject(CDefinition def, String newFieldName, List<CIndex> existingIndexes){
		CField theNewField = def.getField(newFieldName);
		List<CQLStatement> ret = Lists.newArrayList();
		//alter statement for the static table
		ret.add(makeCQLforAddFieldToTable(makeTableName(def,null), theNewField));

		//now make the alter statements for the indexes
		for(CIndex i: existingIndexes){
			ret.add(makeCQLforAddFieldToTable(makeTableName(def,i),theNewField));
		}

		return new BoundedCQLStatementIterator(ret);
	}


	/**
	 *
	 * @return String of single CQL statement required to create the Shard Index Table
	 */
	public CQLStatement makeCQLforShardIndexTableDrop(){
		return CQLStatement.make(String.format(TEMPLATE_DROP, this.keyspace, CObjectShardList.SHARD_INDEX_TABLE_NAME),
				CObjectShardList.SHARD_INDEX_TABLE_NAME);
	}

	/**
	 *
	 * @return String of single CQL statement required to create the Shard Index Table
	 */
	public CQLStatement makeCQLforShardIndexTableTruncate(){
		return CQLStatement.make(String.format(TEMPLATE_TRUNCATE, this.keyspace, CObjectShardList.SHARD_INDEX_TABLE_NAME),
				CObjectShardList.SHARD_INDEX_TABLE_NAME);
	}

	/**
	 *
	 * @return CQLStatement of single CQL statement required to get the first update token
	 */
	public CQLStatement makeGetFirstEligibleIndexUpdate(){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_FIRST_ELIGIBLE_INDEX_UPDATE, keyspace),
			INDEX_UPDATES_TABLE_NAME, Arrays.asList(getTimeUUIDAtEndOfConsistencyHorizion()).toArray());
	}

	/**
	 *
	 * @param lastInstanceKey - Row Key representing the position of the previous row key
	 * @return CQLStatement of the single CQL statement required to get the next update token
	 */
	public CQLStatement makeGetNextEligibleIndexUpdate(IndexUpdateRowKey lastInstanceKey){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_NEXT_ELIGIBLE_INDEX_UPDATE, keyspace),
				INDEX_UPDATES_TABLE_NAME,
				Arrays.asList(lastInstanceKey.getObjectName(),lastInstanceKey.getInstanceId(),getTimeUUIDAtEndOfConsistencyHorizion()).toArray());
	}

	/**
	 *
	 * @param instanceKey - Row Key representing the row key for the row to retrieve
	 * @return CQLStatement of the single CQL statement required to get the Row corresponding to the token
	 */
	public static CQLStatement makeGetRowIndexUpdate(String keyspace, IndexUpdateRowKey instanceKey){
		return CQLStatement.make(String.format(TEMPLATE_SELECT_ROW_INDEX_UPDATE, keyspace),
				INDEX_UPDATES_TABLE_NAME,
				Arrays.asList(instanceKey.getObjectName(),instanceKey.getInstanceId()).toArray());
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
		CIndex i = def.getIndex(indexValues, false);
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
		return CQLStatement.make(query, CObjectShardList.SHARD_INDEX_TABLE_NAME, values.toArray());
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
        return CQLStatement.make(String.format(TEMPLATE_SET_COMPACTION_LEVELED, keyspace, table, sstableSize), table);
    }

    /**
     * @param table - The table to update with the compaction strategy
     * @param minThreshold - minimum number of SSTables to trigger a minor compaction
     * @return String of single CQL statement required to set
     */
    public static CQLStatement makeCQLforTieredCompaction(String keyspace, String table, Integer minThreshold){
        return CQLStatement.make(String.format(TEMPLATE_SET_COMPACTION_TIERED, keyspace, table, minThreshold), table);
    }

	public CQLStatement makeCQLforIndexUpdateTableCreate(){
		return CQLStatement.make(String.format(TEMPLATE_CREATE_INDEX_UPDATES, this.keyspace), INDEX_UPDATES_TABLE_NAME);
	}

	public CQLStatement makeCQLforIndexUpdateTableTruncate(){
		return CQLStatement.make(String.format(TEMPLATE_TRUNCATE_INDEX_UPDATES, this.keyspace), INDEX_UPDATES_TABLE_NAME);
	}

	public static CQLStatementIterator makeCQLforUpdate(String keyspace, CDefinition def, UUID key, Map<String,Object> oldValues, Map<String, Object> newValues) throws CQLGenerationException {
		List<CQLStatement> ret = Lists.newArrayList();
		//(1) Detect if there are any changed index values in values
		List<CIndex> affectedIndexes = getAffectedIndexes(def, oldValues, newValues);
		List<CIndex> unaffectedIndexes = getUnaffectedIndexes(def, oldValues, newValues);

		//(2) Construct a complete copy of the object
		Map<String,Object> completeValues = Maps.newHashMap(oldValues);
		for(String k : newValues.keySet()){
			completeValues.put(k, newValues.get(k));
		}
		Map<String,ArrayList> fieldsAndValues = makeFieldAndValueList(def, completeValues);

		//(3) Delete from any indexes that are no longer applicable
		for(CIndex i : affectedIndexes){
			Map<String,Object> compositeKeyToDelete = i.getIndexKeyAndValues(oldValues);

			//just ignore the delete if the old index values are the same as the new index values.
			//We will update with the new fields later on in the method. Ignoring the delete in this
			//case will solve the problem of async deletes and inserts happening in parallel
			Map<String,Object> compositeKeyOfCompleteValues = i.getIndexKeyAndValues(completeValues);
			if(compositeKeyOfCompleteValues.equals(compositeKeyToDelete)){
				continue;
			}

			//ok we can move forward with the delete now
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to delete on this index. If not just continue
				// because it would be ignored on insert
				if(!i.validateIndexKeys(compositeKeyToDelete)){
					continue;
				}
			}
			ret.add(makeCQLforDeleteUUIDFromIndex(keyspace, def, i, key, compositeKeyToDelete, null));
		}

		//(4) Add index values to the new values list
		Map<String,Object> newValuesAndIndexValues = Maps.newHashMap(newValues);
		for(String s: def.getRequiredFields()){
			if(!newValuesAndIndexValues.containsKey(s)){
				newValuesAndIndexValues.put(s, completeValues.get(s));
			}
		}
		Map<String,ArrayList> fieldsAndValuesForNewValuesAndIndexValues = makeFieldAndValueList(def,newValuesAndIndexValues);

		//(5) Insert into the new indexes like a new insert
		for(CIndex i: affectedIndexes){
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to insert on this index. If not just continue;
				if(!i.validateIndexKeys(i.getIndexKeyAndValues(completeValues))){
					continue;
				}
			}
			addCQLStatmentsForIndexInsert(keyspace, true, ret, def, completeValues, i, key, fieldsAndValues,null, null);
		}

		//(6) Insert into the existing indexes without the shard index addition
		for(CIndex i: unaffectedIndexes){
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

	public static List<CIndex> getAffectedIndexes(CDefinition def, Map<String,Object> oldValues, Map<String,Object> newValues){
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

	public static List<CIndex> getUnaffectedIndexes(CDefinition def, Map<String,Object> oldValues, Map<String,Object> newValues){
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

	public CQLStatementIterator makeCQLforCreate(CDefinition def){
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

	protected static CQLStatementIterator makeCQLforTruncate(String keyspace, CDefinition def){
		List<CQLStatement> ret = Lists.newArrayList();
		ret.add(makeTableTruncate(keyspace, def.getName()));
		if(def.getIndexes() != null) {
			for(CIndex i : def.getIndexes().values()){
				ret.add(makeTableTruncate(keyspace, makeTableName(def, i)));
			}
		}
		return new BoundedCQLStatementIterator(ret);
	}

	protected static CQLStatement makeInsertStatementStatic(String keyspace, String tableName, List<String> fields, List values, Object id, Long timestamp, Integer ttl){
		fields.add(0,"id");
		values.add(0, id);
		String query = String.format(
				TEMPLATE_INSERT_STATIC,
                keyspace,
				tableName,
				makeCommaList(fields),
				makeCommaList(values, true),
				//timestamp.toString(), //add timestamp back when timestamps become preparable
				(ttl == null) ? "" : (" USING TTL "+ttl)//(" AND TTL "+ttl) //Revert this back to AND when timestamps are preparable
		);

		return CQLStatement.make(query, tableName, values.toArray());
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
		return CQLStatement.make(String.format(TEMPLATE_INSERT_INDEX_UPDATES,keyspace),
				tableName,
				Arrays.asList(id, tableName, instanceId, indexValuesAsJson).toArray() );
	}

	protected static CQLStatement makeInsertStatementWide(String keyspace, String tableName, List<String> fields, List<Object> values, Object uuid, long shardid, Long timestamp, Integer ttl){
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

		return CQLStatement.make(query, tableName, values.toArray());
	}

	protected static CQLStatement makeInsertStatementWideIndex(String keyspace, String tableName, String targetTableName, long shardId, List indexValues, Long timestamp) throws CQLGenerationException {
		String indexValuesString = makeIndexValuesString(indexValues);
		Object[] values = {targetTableName, indexValuesString, Long.valueOf(shardId), shardId+":"+indexValuesString};
		return CQLStatement.make(
				String.format(
					TEMPLATE_INSERT_WIDE_INDEX,
					keyspace,
					tableName
					//timestamp.toString() //Add back timestamp when timestamps become preparable
				),
				tableName,
				values);
	}

	public static CQLStatementIterator makeCQLforInsertKeyspaceDefinition(@NotNull String keyspace, @NotNull String name, @NotNull String keyspaceDefinition, @NotNull UUID id) throws CQLGenerationException{
		ArrayList<CQLStatement> ret = Lists.newArrayList();
		ret.add(CQLStatement.make(String.format(TEMPLATE_INSERT_KEYSPACE, keyspace),
				KEYSPACE_DEFINITIONS_TABLE_NAME,
				Arrays.asList(id, name, keyspaceDefinition).toArray()));
		return new BoundedCQLStatementIterator(ret);
	}

	protected static CQLStatementIterator makeCQLforInsert(@NotNull String keyspace, @NotNull CDefinition def, @NotNull Map<String,Object> data) throws CQLGenerationException{
		return makeCQLforInsert(keyspace, def, data, null, null, null);
	}

	protected static CQLStatementIterator makeCQLforInsert(@NotNull String keyspace, @NotNull CDefinition def, @NotNull Map<String,Object> data, @Nullable Object uuid, Long timestamp, Integer ttl) throws CQLGenerationException{
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

	public static void addCQLStatmentsForIndexInsert(String keyspace, boolean includeShardInsert, List<CQLStatement> statementListToAddTo, CDefinition def, @NotNull Map<String,Object> data, CIndex i, Object uuid, Map<String,ArrayList> fieldsAndValues,Long timestamp, Integer ttl) throws CQLGenerationException {
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

	protected static CQLStatement makeCQLforTableScan(String keyspace, CDefinition def){
		return CQLStatement.make(String.format(TEMPLATE_TABLE_SCAN, keyspace, def.getName()), def.getName());
	}

	public static CQLStatement makeCQLforGetKeyspaceDefinitions(String keyspace, String name){
		String statement = String.format(TEMPLATE_SELECT_KEYSPACE, keyspace, name);
		Object[] values = {name};
		return CQLStatement.make(statement, KEYSPACE_DEFINITIONS_TABLE_NAME, values);
	}

	protected static CQLStatementIterator makeCQLforDelete(String keyspace, CDefinition def, UUID key, Map<String,Object> data, Long timestamp){
		if(timestamp == null){
			timestamp = Long.valueOf(System.currentTimeMillis());
		}
		List<CQLStatement> ret = Lists.newArrayList();
		ret.add(makeCQLforDeleteUUIDFromStaticTable(keyspace, def, key, timestamp));
		for(CIndex i : def.getIndexes().values()){
			if(def.isAllowNullPrimaryKeyInserts()){
				//check if we have the necessary primary fields to insert on this index. If not just continue;
				if(!i.validateIndexKeys(i.getIndexKeyAndValues(data))){
					continue;
				}
			}
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
				"id = ?"),
			makeTableName(def, null),
			values);
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
		return CQLStatement.make(query, makeTableName(def,index), values.toArray());
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
		return CQLStatement.make(String.format(TEMPLATE_DROP, keyspace, tableName), tableName);
	}

	protected static CQLStatement makeTableTruncate(String keyspace, String tableName){
		return CQLStatement.make(String.format(TEMPLATE_TRUNCATE, keyspace, tableName), tableName);
	}

	public CQLStatement makeStaticTableCreate(CDefinition def){
		String query = String.format(
			TEMPLATE_CREATE_STATIC,
			keyspace,
			def.getName(),
			def.getPrimaryKeyType(),
			makeFieldList(def.getFields().values(),true));
		return CQLStatement.make(query, def.getName());
	}

	public CQLStatement makeWideTableCreate(CDefinition def, CIndex index){
		String query = String.format(
			TEMPLATE_CREATE_WIDE,
			keyspace,
			makeTableName(def,index),
			def.getPrimaryKeyType(),
			makeFieldList(def.getFields().values(), true),
			makeCommaList(index.getCompositeKeyList()));
		return CQLStatement.make(query, makeTableName(def, index));
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
			if( data.containsKey(f.getName()) && !f.getName().equals("id") ){
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
		return CQLStatement.make(query, def.getName(), values.toArray());
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
			if(f.getName().equals("id")){
				continue; //ignore the id, if this definition specifies an id
			}
			ret = ret + f.getName() +
					(withType ? " " + f.getType() : "") +
					(it.hasNext() ? "," : "");
		}
		return ret;
	}

	public static String makeTableName(CDefinition def, @Nullable CIndex index){
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