package com.pardot.rhombus.cli.commands;

import com.pardot.rhombus.cobject.CQLGenerationException;
import org.apache.commons.cli.CommandLine;

/**
 * User: Rob Righter
 * Date: 8/19/13
 */
public class PrepareInserts extends RcliWithCassandraConfig {


    public boolean executeCommand(CommandLine cl){

	    boolean ret = super.executeCommand(cl);
	    if(!ret){
		    return false;
	    }
        //now rebuild the keyspace
        try{
            getConnectionManager().setDefaultKeyspace(keyspaceDefinition);
            getConnectionManager().getObjectMapper().prePrepareInsertStatements();
            //looks like a success. Lets go ahead and return as such
            return true;
        }
        catch (CQLGenerationException e){
            System.out.println(e);
            System.out.println("Error encountered while attempting to prepare the Insert Statements for the keyspace");
	        return false;
        }

    }
}
