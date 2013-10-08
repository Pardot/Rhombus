package com.pardot.rhombus.cobject;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.shardingstrategy.TimebasedShardingStrategy;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

/**
 * Pardot, An ExactTarget Company.
 * User: robrighter
 * Date: 4/4/13
 */
public class CIndex {

	private String key;
	private List<String> compositeKeyList;

	private TimebasedShardingStrategy shardingStrategy;

	public CIndex() {

	}

	public CIndex(String name, String key, TimebasedShardingStrategy shardingStrategy){
		this.setKey(key);
		this.shardingStrategy = shardingStrategy;
	}


	/**
	 * Determine if the keys provided can be constructed
	 * to form a composite key for this index. In other words
	 * this method answers the question "Can I query this index
	 * using this criteria"
	 * @return boolean - true if it is queryable
	 */
	public boolean validateIndexKeys(Map<String,Object> keys){
		if(keys.size() != compositeKeyList.size()){
			//optimized return if we have a size mismatch
			return false;
		}
		for(String s : compositeKeyList){
			if(keys.get(s) == null){
				return false;
			}
		}
		return true;
	}

	public boolean areValuesAssociatedWithIndex(Map<String,Object> values){
		for(String key: compositeKeyList){
			if(values.containsKey(key)){
				return true;
			}
		}
		return false;
	}

	public TimebasedShardingStrategy getShardingStrategy(){
		return shardingStrategy;
	}

	public void setShardingStrategy(TimebasedShardingStrategy shardingStrategy){
		this.shardingStrategy = shardingStrategy;
	}

	@JsonIgnore
	public String getName() {
		return getKey();
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		ArrayList<String> listtoset = new ArrayList<String>(Arrays.asList(key.split("\\s*:\\s*")));
		java.util.Collections.sort(listtoset);
		this.compositeKeyList = listtoset;
		this.key = Joiner.on(":").join(listtoset);
	}

	@JsonIgnore
	public List<String> getCompositeKeyList() {
		return compositeKeyList;
	}

	@JsonIgnore
	public List<Object> getIndexValues(Map<String,Object> allValues){
		List<Object> ret = Lists.newArrayList();
		for(String key : compositeKeyList){
			ret.add(allValues.get(key));
		}
		return ret;
	}

	@JsonIgnore
	public SortedMap<String,Object> getIndexKeyAndValues(Map<String,Object> allValues){
		SortedMap<String,Object> ret = Maps.newTreeMap();
		for(String key : compositeKeyList){
			ret.put(key, allValues.get(key));
		}
		return ret;
	}

}
