package com.pardot.rhombus.cobject;



import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.util.MapToListSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.*;

/**
 * Pardot, An ExactTarget Company.
 * User: robrighter
 * Date: 4/4/13
 */
public class CDefinition {

	private String name;

	@JsonSerialize(using = MapToListSerializer.class)
	@JsonProperty
	private Map<String, CField> fields;

	@JsonSerialize(using = MapToListSerializer.class)
	@JsonProperty
	private Map<String, CIndex> indexes;

	@JsonIgnore
	private SortedMap<String, CIndex> indexesIndexedByFields;

	private boolean allowNullPrimaryKeyInserts = false;

	public CDefinition(){
	}

	public static CDefinition fromJsonString(String json) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(json, CDefinition.class);
	}

	//Getters and setters for Jackson
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Map<String, CField> getFields() {
		return fields;
	}
	public void setFields(List<CField> fields) {
		this.fields = Maps.newHashMap();
		for(CField field : fields) {
			this.fields.put(field.getName(), field);
		}
	}
	public Map<String, CIndex> getIndexes() {
		return indexes;
	}
	public void setIndexes(List<CIndex> indexes) {
		this.indexes = Maps.newHashMap();
		this.indexesIndexedByFields = Maps.newTreeMap();
		for(CIndex index : indexes) {
			this.indexes.put(index.getName(), index);
			this.indexesIndexedByFields.put(index.getKey(),index);
		}
	}

	public boolean isAllowNullPrimaryKeyInserts() {
		return allowNullPrimaryKeyInserts;
	}

	public void setAllowNullPrimaryKeyInserts(boolean allowNullPrimaryKeyInserts) {
		this.allowNullPrimaryKeyInserts = allowNullPrimaryKeyInserts;
	}

	@JsonIgnore
	public Collection<String> getRequiredFields(){
		Map<String,String> ret = Maps.newHashMap();
		if(indexes != null) {
			for( CIndex i : indexes.values()){
				for(String key : i.getCompositeKeyList()){
					ret.put(key,key);
				}
			}
		}
		return ret.values();
	}

	public Map<String, Object> makeIndexValues(Map<String,Object> allValues){
		Map<String,Object> ret = Maps.newTreeMap();
		Collection<String> requiredFields = getRequiredFields();
		for(String f: requiredFields){
			ret.put(f, allValues.get(f));
		}
		return ret;
	}

	public CIndex getIndex(SortedMap<String,Object> indexValues){
		String key = Joiner.on(":").join(indexValues.keySet());
		return indexesIndexedByFields.get(key);
	}

	@JsonIgnore
	public List<CIndex> getIndexesAsList(){
		List<CIndex> ret = Lists.newArrayList();
		for(String key: this.indexes.keySet()){
			ret.add(this.indexes.get(key));
		}
		return ret;
	}

	public CField getField(String fieldName) {
		return fields.get(fieldName);
	}

	@JsonIgnore
	public String getPrimaryKeyType(){
		if(fields.containsKey("id")){
			return fields.get("id").getType().toString();
		}
		else{
			return CField.CDataType.TIMEUUID.toString();
		}
	}

	@JsonIgnore
	public Class getPrimaryKeyClass(){
		if(fields.containsKey("id")){
			CField idField = fields.get("id");
			return idField.getEmptyJavaObjectOfThisType().getClass();
		}
		else{
			return UUID.class;
		}
	}
}
