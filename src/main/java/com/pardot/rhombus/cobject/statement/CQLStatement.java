package com.pardot.rhombus.cobject.statement;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 6/10/13
 */
public class CQLStatement implements Comparable<CQLStatement>{
	private String query;
	private Object[] values;
	private boolean isCacheable = false;

	public static CQLStatement make(String query){
		return new CQLStatement(query);
	}

	public static CQLStatement make(String query, Object[] values){
		return new CQLStatement(query,values);
	}

	private CQLStatement(){
	}

	private CQLStatement(String query, Object[] values){
		this.query = query;
		this.values = values;
	}

	private CQLStatement(String query){
		this.query = query;
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
	public boolean equals(Object obj) {
		if(!(obj instanceof CQLStatement)){
			return false;
		}
		CQLStatement o = (CQLStatement)obj;
		if(!this.getQuery().equals(o.getQuery())){
			return false;
		}
		if(!(this.isPreparable() == o.isPreparable())){
			return false;
		}
		if((this.getValues() == null) || (o.getValues() == null)){
			return (this.getValues() == o.getValues());
		}
		if(this.getValues().length != o.getValues().length){
			return false;
		}
		for(int i = 0; i<this.getValues().length; i++){
			if(!this.getValues()[i].equals(o.getValues()[i])){
				return false;
			}
		}
		return true;
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
}
