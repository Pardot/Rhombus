package com.pardot.rhombus.cobject.statement;
import com.google.common.base.Objects;

import java.util.Arrays;


public class CQLStatement implements Comparable<CQLStatement>{
	private String query;
	private String objectName;
	private Object[] values;
	private boolean isCacheable = false;

	public static CQLStatement make(String query, String objectName){
		return new CQLStatement(query, objectName);
	}

	public static CQLStatement make(String query, String objectName, Object[] values){
		return new CQLStatement(query, objectName, values);
	}

	private CQLStatement(){

	}

	private CQLStatement(String query, String objectName, Object[] values){
		this.query = query;
		this.objectName = objectName;
		this.values = values;
	}

	private CQLStatement(String query, String objectName){
		this.query = query;
		this.objectName = objectName;
		this.values = null;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Object[] getValues() {
		return values;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}

	public boolean isPreparable() {
		return (values != null);
	}

	public int compareTo(CQLStatement o){
		if(this.equals(o)){
			return 0;
		}
		if(this.getQuery().equals(o.getQuery())){
			return -1;
		}
		else{
			return 1;
		}
	}

	@Override
	public boolean equals(Object otherObject) {
		if(otherObject == null) {
			return false;
		}
		if(getClass() != otherObject.getClass()) {
			return false;
		}
		CQLStatement otherStatement = (CQLStatement)otherObject;
		return (
				Objects.equal(this.getQuery(), otherStatement.getQuery())
				&& Arrays.equals(this.getValues(), otherStatement.getValues())
				//&& Objects.equal(this.getObjectName(), otherStatement.getObjectName())
				&& this.isPreparable() == otherStatement.isPreparable()
				&& this.isCacheable() == otherStatement.isCacheable()
				);
	}

	@Override
	public String toString() {
		String ret = "Query: "+this.getQuery()+"\n";
		ret+= "Values: ";
		if(values != null){
			ret+= "[\n";
			for(int i=0;i<this.getValues().length;i++){
				ret+="    "+this.getValues()[i].toString()+" ("+this.getValues()[i].getClass()+") "+ (i+1<this.getValues().length ? "," : "") +"\n";
			}
			ret+="\n]";
		}
		else{
			ret+="null";
		}
		ret+="\nPreparable: "+this.isPreparable();

		return ret;
	}

	public boolean isCacheable() {
		return isCacheable;
	}

	public void setCacheable(boolean cacheable) {
		isCacheable = cacheable;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
}
