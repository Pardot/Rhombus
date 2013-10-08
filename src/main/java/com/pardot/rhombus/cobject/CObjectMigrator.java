package com.pardot.rhombus.cobject;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * User: Rob Righter
 * Date: 10/8/13
 */
public class CObjectMigrator {

	protected CDefinition OldDefinition;
	protected CDefinition NewDefinition;

	public CObjectMigrator(CDefinition oldDefinition, CDefinition newDefinition) {
		this.OldDefinition = oldDefinition;
		this.NewDefinition = newDefinition;
	}

	public boolean isMigratable() {
		//currently we only support adding new indexs and NOT new fields
		if(OldDefinition.getFields().size() == NewDefinition.getFields().size()){
			for(CField f: NewDefinition.getFields().values()){
				if( (!OldDefinition.getFields().containsKey(f.getName())) ||
					(!OldDefinition.getFields().get(f.getName()).getType().equals(f.getType())) ){
					return false;
				}
			}
		}
		else{
			return false;
		}

		//currently we only allow new indexes
		for( CIndex i : OldDefinition.getIndexes().values() ){
			if(!NewDefinition.getIndexes().containsKey(i.getKey())){
				//we are missing one of our old definitions
				return false;
			}

			if(!NewDefinition.getIndexes().get(i.getKey()).getShardingStrategy().getClass().equals(i.getShardingStrategy().getClass())){
				//this index changed sharding strategy. We do not support that yet
				return false;
			}

		}
		// Looks like we are good to migrate this CDefinition
		return true;
	}

	public CQLStatementIterator getMigrationCQL() throws CObjectMigrationException {
		if(!isMigratable()){
			throw new CObjectMigrationException("CDefinition migration requested for "+NewDefinition.getName()+ " is not currently supported");
		}
		List<CQLStatement> ret = Lists.newArrayList();
		List<CIndex> newIndexes = getNewIndexes();
		for(CIndex i: newIndexes){
			ret.add(CObjectCQLGenerator.makeWideTableCreate(NewDefinition,i));
		}
		return new BoundedCQLStatementIterator(ret);
	}

	public List<CIndex> getNewIndexes(){
		List<CIndex> ret = Lists.newArrayList();
		for( CIndex i : NewDefinition.getIndexes().values() ){
			if(!OldDefinition.getIndexes().containsKey(i.getKey())){
				//this index is not in the old definition, so add it as a new one
				ret.add(i);
			}
		}
		return ret;
	}


}
