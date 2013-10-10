package com.pardot.rhombus.cli.commands;

import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * User: Rob Righter
 * Date: 10/9/13
 */
public class RcliWithExistingKeyspace extends RcliWithCassandraConfig {

	public String keyspaceName;
	public ObjectMapper objectMapper;

	public Options getCommandOptions(){
		Options ret = super.getCommandOptions();
		Option keyspaceName = OptionBuilder.withArgName("name")
				.hasArg()
				.withDescription("Name of the existing keyspace to be used")
				.create( "keyspace" );
		ret.addOption(keyspaceName);
		return ret;
	}

	public boolean executeCommand(CommandLine cl){
		boolean ret = super.executeCommand(cl);
		if(!ret){
			return false;
		}

		if(!cl.hasOption("keyspace")){
			displayHelpMessage();
			return false;
		}
		this.keyspaceName = cl.getOptionValue("keyspace");
		ConnectionManager cm = getConnectionManager();
		this.objectMapper = cm.getObjectMapper(this.keyspaceName);
		return true;
	}

}
