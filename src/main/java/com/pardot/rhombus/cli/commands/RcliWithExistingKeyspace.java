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

	public void executeCommand(CommandLine cl){
		super.executeCommand(cl);

		if(!cl.hasOption("keyspace")){
			displayHelpMessageAndExit();
		}
		this.keyspaceName = cl.getOptionValue("keyspace");
		ConnectionManager cm = getConnectionManager();
		this.objectMapper = cm.getObjectMapper(this.keyspaceName);
	}

}
