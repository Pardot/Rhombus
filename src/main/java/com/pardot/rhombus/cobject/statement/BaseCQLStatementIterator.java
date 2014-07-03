package com.pardot.rhombus.cobject.statement;

import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.statement.CQLStatement;

import java.util.Map;
import java.util.UUID;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 2/4/14
 */
public abstract class BaseCQLStatementIterator implements CQLStatementIterator {
	private Map<String, Object> clientFilters;
	protected long limit = 0;
	protected long currentShardId = -1;
	protected UUID nextUuid = null;
	CObjectOrdering ordering = null;
	protected int startUUidIndex = 0;
	protected int endUuidIndex = 0;

	public Map<String, Object> getClientFilters() {
		return clientFilters;
	}

	public void setClientFilters(Map<String, Object> clientFilters) {
		this.clientFilters = clientFilters;
	}

	public void setLimit(long limit){
		this.limit = limit;
	}

	public void setNextUuid(UUID uuid){

		this.nextUuid = uuid;
	}

	public void nextShard(){
		currentShardId = 1;
	}

	protected boolean hasStartUuid(String query){

		if (query.contains("id >")){

			return true;
		}

		return false;
	}

	protected boolean hasEndUUid(String query){

		if (query.contains("id <")){

			return true;
		}

		return false;
	}

	protected void setStartEndUuidIndexes(CQLStatement cqlStatement, Object[] values){

		boolean hasStartUuid = hasStartUuid(cqlStatement.getQuery());
		boolean hasEndUuid = hasEndUUid(cqlStatement.getQuery());

		if (hasStartUuid && !hasEndUuid){
			this.startUUidIndex = values.length - 1;
		} else if (!hasStartUuid && hasEndUuid){
			this.endUuidIndex = values.length - 1;
		} else if(hasStartUuid && hasEndUuid){
			this.startUUidIndex = values.length - 2;
			this.endUuidIndex =  values.length - 1;
		}
	}

	protected String setIdClausesToBeInclusive(String query){

		if (!query.contains("id >=")){

			query = query.replace("id >", "id >=");
		}

		if (!query.contains("id <=")){

			query = query.replace("id <", "id <=");
		}

		return query;
	}

	protected String insertStartUuidClause(String query){
		String idClause = "AND id <";
		String[] parts = query.split(idClause);

		if (parts[1].contains("=")){
			idClause = "AND id >= ? " + idClause;
		} else{
			idClause = "AND id > ? " + idClause;
		}

		query = parts[0] + idClause + parts[1];

		return query;
	}
}
