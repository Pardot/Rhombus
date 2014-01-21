package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 7/5/13
 */
public class UpdateProcessor {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapper.class);
	private ObjectMapper objectMapper;

	public UpdateProcessor(ObjectMapper om){
		this.objectMapper = om;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void process() throws IOException {
		IndexUpdateRow row = objectMapper.getNextUpdateIndexRow(null);
		while(row != null){
			processRow(row);
			row = objectMapper.getNextUpdateIndexRow(row.getRowKey());
		}
	}

	public List<Map<String,Object>> getUpdatesThatHappenedWithinTimeframe(Long timeInNanos) throws IOException {
		return getUpdatesThatHappenedWithinTimeframe(timeInNanos, 0l);
	}

	public List<Map<String,Object>> getUpdatesThatHappenedWithinTimeframe(Long timeInNanos, long rowLimit) throws IOException {
		logger.info("rowLimit: {} ", rowLimit);
		List<Map<String,Object>> ret = Lists.newArrayList();
		IndexUpdateRow row = objectMapper.getNextUpdateIndexRow(null);
		long examinedRows = 0l;
		while(row != null){
			List<Map<String,Object>> toadd = findUpdatesWithinTimeframe(row,timeInNanos);
			if(toadd.size() > 0){
				ret.addAll(toadd);
			}
			examinedRows++;
			if(rowLimit > 0 && examinedRows >= rowLimit) {
				break;
			}
			row = objectMapper.getNextUpdateIndexRow(row.getRowKey());
		}
		return ret;
	}

	protected List<Map<String,Object>> findUpdatesWithinTimeframe(IndexUpdateRow row, Long timeInNannos){
		List<Map<String,Object>> ret = Lists.newArrayList();
		UUID newer = null;
		int i = 0;
		for(UUID current : row.getIds()){
			if(newer != null){
				///do stuff
				Long difference = newer.timestamp() - current.timestamp();
				if(difference < timeInNannos){
					Map<String,Object> toadd = Maps.newHashMap();
					toadd.put("rowkey", row.getRowKey());
					toadd.put("new-item", row.getIndexValues().get(i));
					toadd.put("old-item", row.getIndexValues().get(i-1));
					toadd.put("difference", difference);
					ret.add(toadd);
				}
			}
			newer = current;
			i++;
		}
		return ret;
	}

	protected void processRow(IndexUpdateRow row){
		if(row.getIndexValues().size() == 0){
			return;
		}
		if(row.getIndexValues().size() == 1){
			//if this is older than the consistency horizon, just delete it
			Long consistencyHorizon = UUIDs.unixTimestamp(objectMapper.getTimeUUIDAtEndOfConsistencyHorizion());
			if(row.getTimeStampOfMostCurrentUpdate() > consistencyHorizon){
				objectMapper.deleteObsoleteUpdateIndexColumn(row.getRowKey(),row.getIds().get(0));
			}
			return;
		}

		//make a list of all the updated indexes and subtract all the indexes that are current
		Map<String,Object> mostRecentUpdate = row.getIndexValues().get(0);
		row.getIndexValues().remove(0);
		row.getIds().remove(0);
		List<CIndex> listOfIndexesToDelete = Lists.newArrayList();
		List<Map<String,Object>> listOfValuesToDelete = Lists.newArrayList();
		for(Map<String,Object> update: row.getIndexValues()){
			if(!areIndexValuesEqual(mostRecentUpdate, update)){
				listOfValuesToDelete.add(update);
				listOfIndexesToDelete.addAll(
					getListOfEffectedIndexes(objectMapper.getKeyspaceDefinition().getDefinitions().get(row.getObjectName()),
					mostRecentUpdate,
					update));
			}
		}
		//delete the list of indexes with a timestamp of the current update
		for(CIndex index : listOfIndexesToDelete){
			for(Map<String,Object> values: listOfValuesToDelete){
				objectMapper.deleteObsoleteIndex(row,index, values);
			}
		}

		//now delete the processed update columns in this row
		for(UUID todelete: row.getIds()){
			objectMapper.deleteObsoleteUpdateIndexColumn(row.getRowKey(),todelete);
		}

	}

	//todo add unit test
	protected List<CIndex> getListOfEffectedIndexes(CDefinition def, Map<String,Object> a, Map<String,Object> b){
		List<CIndex> ret = Lists.newArrayList();
		for(CIndex i: def.getIndexesAsList()){
			Map<String,Object> aValues = i.getIndexKeyAndValues(a);
			Map<String,Object> bValues = i.getIndexKeyAndValues(b);
			if(!areIndexValuesEqual(aValues,bValues)){
				ret.add(i);
			}
		}
		return ret;
	}

	//todo add unit test
	protected boolean areIndexValuesEqual(Map<String,Object> a, Map<String,Object> b){
		if(a.keySet().size() != b.keySet().size()){
			return false;
		}
		for(String key: a.keySet()){
			if(a.get(key) != b.get(key)){
				return false;
			}
		}
		return true;
	}


}
