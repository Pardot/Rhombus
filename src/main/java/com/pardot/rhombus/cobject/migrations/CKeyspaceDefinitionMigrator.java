package com.pardot.rhombus.cobject.migrations;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.*;

import java.util.List;

/**
 * User: Rob Righter
 * Date: 10/8/13
 */
public class CKeyspaceDefinitionMigrator {

	CKeyspaceDefinition OldKeyspace;
	CKeyspaceDefinition NewKeyspace;

	public CKeyspaceDefinitionMigrator(CKeyspaceDefinition oldKeyspace, CKeyspaceDefinition newKeyspace){
		this.OldKeyspace = oldKeyspace;
		this.NewKeyspace = newKeyspace;
	}

	public boolean isMigratable(){
		//make sure we keep the same name
		if(!OldKeyspace.getName().equals(NewKeyspace.getName())){
			return false;
		}
		//currently we support adding new objects and migrating old objects
		for(CDefinition def : NewKeyspace.getDefinitions().values()){
			if(OldKeyspace.getDefinitions().containsKey(def.getName())){
				CObjectMigrator m = new CObjectMigrator(OldKeyspace.getDefinitions().get(def.getName()),def);
				if(!m.isMigratable()){
					return false;
				}
			}
		}
		return true;
	}

	public CQLStatementIterator getMigrationCQL() throws CObjectMigrationException {
		if(!isMigratable()){
			throw new CObjectMigrationException("CKeyspaceDefinition migration requested for "+NewKeyspace.getName()+ " is not currently supported");
		}
		List<CQLStatementIterator> its = Lists.newArrayList();
		for(CDefinition def : NewKeyspace.getDefinitions().values()){
			if(OldKeyspace.getDefinitions().containsKey(def.getName())){
				CObjectMigrator m = new CObjectMigrator(OldKeyspace.getDefinitions().get(def.getName()),def);
				its.add(m.getMigrationCQL());
			}
			else {
				its.add(CObjectCQLGenerator.makeCQLforCreate(def));
			}
		}

		String keyspaceDefinitionAsJson;
		try{
			com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
			keyspaceDefinitionAsJson = om.writeValueAsString(NewKeyspace);
			its.add(CObjectCQLGenerator.makeCQLforInsertKeyspaceDefinition(NewKeyspace.getName(), keyspaceDefinitionAsJson, UUIDs.timeBased()));
		}
		catch(Exception e) {
			throw new CObjectMigrationException(e.getMessage());
		}

		BoundedCQLStatementIterator ret = null;
		try{
			ret = BoundedCQLStatementIterator.condenseIterators(its);
		}
		catch (CQLGenerationException e){
			throw new CObjectMigrationException(e.getMessage());
		}
		return ret;
	}

}
