package com.pardot.rhombus.cobject;

import com.datastax.driver.core.ConsistencyLevel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.pardot.rhombus.util.MapToListSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


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

	@Override
	public boolean equals(Object other) {
		if(other == null) {
			return false;
		}
		if(this.getClass() != other.getClass()) {
			return false;
		}
		final CKeyspaceDefinition otherCKeyspaceDefinition = (CKeyspaceDefinition)other;
		return Objects.equal(this.getName(), otherCKeyspaceDefinition.getName())
				&& Objects.equal(this.getReplicationClass(), otherCKeyspaceDefinition.getReplicationClass())
				&& Objects.equal(this.getConsistencyLevel(), otherCKeyspaceDefinition.getConsistencyLevel())
				&& Objects.equal(this.getReplicationFactors(), otherCKeyspaceDefinition.getReplicationFactors())
				&& Objects.equal(this.getDefinitions(), otherCKeyspaceDefinition.getDefinitions());
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
