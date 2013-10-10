package com.pardot.rhombus.cli.commands;

import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.cli.RhombusCli;
import com.pardot.rhombus.cli.RhombusCommand;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.commons.cli.*;

import java.io.IOException;

/**
 * User: Rob Righter
 * Date: 8/17/13
 */
public class RcliWithCassandraConfig extends RhombusCli {

    private ConnectionManager connectionManager = null;

    public Options getCommandOptions(){
        Options ret = super.getCommandOptions();
        Option cassConfig = OptionBuilder.withArgName( "filename" )
                .hasArg()
                .withDescription("Filename of json Cassandra Configuration")
                .create( "cassconfig" );
        Option logcql = new Option( "c", "Log cql as it runs" );
        ret.addOption(cassConfig);
        ret.addOption(logcql);
        return ret;
    }

    public ConnectionManager getConnectionManager(){
        return connectionManager;
    }

    public boolean executeCommand(CommandLine cl){
        boolean ret = super.executeCommand(cl);
	    if(!ret){
		    return false;
	    }

        if(!cl.hasOption("cassconfig")){
            displayHelpMessage();
	        return false;
        }

        String cassConfigFileName = cl.getOptionValue("cassconfig");

	    CassandraConfiguration cassConfig = null;
        try{
            cassConfig = JsonUtil.objectFromJsonFile(CassandraConfiguration.class, CassandraConfiguration.class.getClassLoader(), cassConfigFileName);
        }
        catch (Exception e){
            System.out.println("Could not parse cassandra configuration file "+cassConfigFileName);
            return false;
        }

        if(cassConfig == null){
            System.out.println("Could not parse cassandra configuration file "+cassConfigFileName);
            return false;
        }

        connectionManager = new ConnectionManager(cassConfig);
        connectionManager.setLogCql(cl.hasOption("c"));
        connectionManager.buildCluster();

        if(connectionManager == null){
            System.out.println("Could create cassandra connection manager from file "+cassConfigFileName);
            return false;
        }
	    return true;

    }
}
