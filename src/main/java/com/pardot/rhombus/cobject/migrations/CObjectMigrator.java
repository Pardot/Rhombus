package com.pardot.rhombus.cobject.migrations;

import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.statement.BoundedCQLStatementIterator;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;

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
		//currently we only support adding new fields not removing old ones
		for(CField f: OldDefinition.getFields().values()){
			if( (!NewDefinition.getFields().containsKey(f.getName())) ||
				(!NewDefinition.getFields().get(f.getName()).getType().equals(f.getType())) ){
				return false;
			}
		}

		if(NewDefinition.getFields().containsKey("id")){
			if(!OldDefinition.getFields().containsKey("id")){
				//You cannot add a custom id in a migration
				return false;
			}
			if(!OldDefinition.getField("id").getType().equals(NewDefinition.getField("id").getType())){
				//You also cannot change the type
				return false;
			}
		}


		//currently we allow new indexes but and removing old ones
		//we do not support changing the sharding strategy
		for( CIndex i : OldDefinition.getIndexes().values() ){
			if(!NewDefinition.getIndexes().get(i.getKey()).getShardingStrategy().getClass().equals(i.getShardingStrategy().getClass())){
				//this index changed sharding strategy. We do not support that yet
				return false;
			}
		}
		// Looks like we are good to migrate this CDefinition
		return true;
	}

	public CQLStatementIterator getMigrationCQL(CObjectCQLGenerator cqlGenerator) throws CObjectMigrationException {
		if(!isMigratable()){
			throw new CObjectMigrationException("CDefinition migration requested for "+NewDefinition.getName()+ " is not currently supported");
		}

		List<CQLStatementIterator> ret = Lists.newArrayList();

		//first migrate the fields
		List<CField> newFields = getNewFields();
		for(CField f: newFields){
			ret.add(cqlGenerator.makeCQLforAddFieldToObject(NewDefinition, f.getName(), OldDefinition.getIndexesAsList()));
		}

		//next migrate the indexes
		List<CQLStatement> indexAdds = Lists.newArrayList();
		List<CIndex> newIndexes = getNewIndexes();
		for(CIndex i: newIndexes){
			indexAdds.add(cqlGenerator.makeWideTableCreate(NewDefinition,i));
		}
		ret.add(new BoundedCQLStatementIterator(indexAdds));

		//note we do not need to do anything with removed indexes. As long as it's removed
		//from the updated cDefinition no more writes will be processed to that table. It
		// is the responsibility of the user to manually truncate and drop tables if desired.

		try{
			return BoundedCQLStatementIterator.condenseIterators(ret);
		}
		catch (CQLGenerationException e){
			throw new CObjectMigrationException("Encountered error attempting to generate CQL for migration");
		}


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

	public List<CField> getNewFields(){
		List<CField> ret = Lists.newArrayList();
		for( CField f : NewDefinition.getFields().values() ){
			if(!OldDefinition.getFields().containsKey(f.getName())){
				//this field is not in the old definition, so add it as a new one
				ret.add(f);
			}
		}
		return ret;
	}

}
