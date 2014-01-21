package com.pardot.rhombus;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 4/17/13
 */
public class CassandraConfiguration {

	@NotNull
	@JsonProperty
	private List<String> contactPoints;

	@NotNull
	@JsonProperty
	private Integer consistencyHorizon;

	@JsonProperty
	private Long batchTimeout;

	@JsonProperty
	private String localDatacenter;

	@JsonProperty
	private Integer maxConnectionPerHostLocal;

	@JsonProperty
	private Integer maxConnectionPerHostRemote;

	@JsonProperty
	private Integer maxSimultaneousRequestsPerConnectionTreshold;

	@JsonProperty
	private Integer individualNodeConnectionTimeout;

	@JsonProperty
	private Integer driverReadTimeoutMillis;

	@JsonProperty
	private String rhombusKeyspaceName;

	public List<String> getContactPoints() {
		return contactPoints;
	}

	public void setContactPoints(List<String> contactPoints) {
		this.contactPoints = contactPoints;
	}

	public String getLocalDatacenter() {
		return localDatacenter;
	}

	public void setLocalDatacenter(String localDatacenter) {
		this.localDatacenter = localDatacenter;
	}

	public Integer getConsistencyHorizon() {
		return consistencyHorizon;
	}

	public void setConsistencyHorizon(Integer consistencyHorizon) {
		this.consistencyHorizon = consistencyHorizon;
	}

	public Long getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(Long batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public Integer getMaxConnectionPerHostLocal() {
		return maxConnectionPerHostLocal;
	}

	public void setMaxConnectionPerHostLocal(Integer maxConnectionPerHostLocal) {
		this.maxConnectionPerHostLocal = maxConnectionPerHostLocal;
	}

	public Integer getMaxConnectionPerHostRemote() {
		return maxConnectionPerHostRemote;
	}

	public void setMaxConnectionPerHostRemote(Integer maxConnectionPerHostRemote) {
		this.maxConnectionPerHostRemote = maxConnectionPerHostRemote;
	}

	public Integer getMaxSimultaneousRequestsPerConnectionTreshold() {
		return maxSimultaneousRequestsPerConnectionTreshold;
	}

	public void setMaxSimultaneousRequestsPerConnectionTreshold(Integer maxSimultaneousRequestsPerConnectionTreshold) {
		this.maxSimultaneousRequestsPerConnectionTreshold = maxSimultaneousRequestsPerConnectionTreshold;
	}

	public Integer getIndividualNodeConnectionTimeout() {
		return individualNodeConnectionTimeout;
	}

	public void setIndividualNodeConnectionTimeout(Integer individualNodeConnectionTimeout) {
		this.individualNodeConnectionTimeout = individualNodeConnectionTimeout;
	}

	public Integer getDriverReadTimeoutMillis() {
		return driverReadTimeoutMillis;
	}

	public void setDriverReadTimeoutMillis(Integer driverReadTimeoutMillis) {
		this.driverReadTimeoutMillis = driverReadTimeoutMillis;
	}

	public String getRhombusKeyspaceName() {
		return rhombusKeyspaceName;
	}

	public void setRhombusKeyspaceName(String rhombusKeyspaceName) {
		this.rhombusKeyspaceName = rhombusKeyspaceName;
	}
}
