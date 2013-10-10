package com.pardot.rhombus.cli.commands;

import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLStatement;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

/**
 * User: Rob Righter
 * Date: 10/9/13
 */
public class RunMigration extends RcliWithExistingKeyspace {

	public Options getCommandOptions(){
		Options ret = super.getCommandOptions();
		Option keyspaceFile = OptionBuilder.withArgName("filename")
				.hasArg()
				.withDescription("Filename of the new keyspace definition")
				.create( "newkeyspacefile" );
		Option keyspaceResource = OptionBuilder.withArgName( "filename" )
				.hasArg()
				.withDescription("Resource filename of the new keyspace definition")
				.create( "newkeyspaceresource" );
		Option list = new Option( "l", "Only list the cql for the migration (does not run the migration)" );
		ret.addOption(keyspaceFile);
		ret.addOption(keyspaceResource);
		return ret;
	}

	public boolean executeCommand(CommandLine cl){
		boolean ret = super.executeCommand(cl);
		if(!ret){
			return false;
		}

		if(!(cl.hasOption("newkeyspacefile") || cl.hasOption("newkeyspaceresource"))){
			displayHelpMessage();
			return false;
		}

		String NewKeyspaceFileName = cl.hasOption("newkeyspacefile") ? cl.getOptionValue("newkeyspacefile") : cl.getOptionValue("newkeyspaceresource");
		//make the keyspace definition
		CKeyspaceDefinition NewkeyDef = null;
		try{
			NewkeyDef = cl.hasOption("newkeyspacefile") ?
					JsonUtil.objectFromJsonFile(CKeyspaceDefinition.class, CKeyspaceDefinition.class.getClassLoader(), NewKeyspaceFileName) :
					JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class,CKeyspaceDefinition.class.getClassLoader(), NewKeyspaceFileName);
		}
		catch (IOException e){
			System.out.println("Could not parse keyspace file "+NewKeyspaceFileName);
			return false;
		}

		if(NewkeyDef == null){
			System.out.println("Could not parse keyspace file "+NewKeyspaceFileName);
			return false;
		}

		//now run the migration
		try{
			if(cl.hasOption("l")){
				//just print out a list of CQL statements for the migration
				List<CQLStatement> torun = this.objectMapper.runMigration(NewkeyDef, true);
				for(CQLStatement c:torun){
					System.out.println(c.getQuery());
				}
			}
			else{
				//actually run the migration
				this.objectMapper.runMigration(NewkeyDef, true);
			}
			return true;
		}
		catch (Exception e){
			System.out.println("Error encountered while attempting to rebuild the keyspace");
			return false;
		}

	}

}
