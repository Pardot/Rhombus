package com.pardot.rhombus.cobject;

import com.datastax.driver.core.ConsistencyLevel;

import com.google.common.collect.Maps;
import com.pardot.rhombus.util.MapToListSerializer;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

public class CKeyspaceDefinition {
	private String name;
	private String replicationClass;
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	private Map<String, Integer> replicationFactors;

	@JsonSerialize(using = MapToListSerializer.class)
	@JsonProperty
	private Map<String, CDefinition> definitions;

	public CKeyspaceDefinition() {

	}

	public static CKeyspaceDefinition fromJsonString(String json) throws IOException {
		com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
		return mapper.readValue(json, CKeyspaceDefinition.class);
	}

	public static CKeyspaceDefinition fromJsonFile(String filename) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		InputStream inputStream = CKeyspaceDefinition.class.getClassLoader().getResourceAsStream(filename);
		return mapper.readValue(inputStream, CKeyspaceDefinition.class);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, CDefinition> getDefinitions() {
		return definitions;
	}

	public void setDefinitions(Collection<CDefinition> definitions) {
		this.definitions = Maps.newHashMap();
		for(CDefinition def : definitions) {
			this.definitions.put(def.getName(), def);
		}
	}

	public String getReplicationClass() {
		return replicationClass;
	}

	public void setReplicationClass(String replicationClass) {
		this.replicationClass = replicationClass;
	}

	public Map<String, Integer> getReplicationFactors() {
		return replicationFactors;
	}

	public void setReplicationFactors(Map<String, Integer> replicationFactors) {
		this.replicationFactors = replicationFactors;
	}

	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}
}
