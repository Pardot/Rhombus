package com.pardot.rhombus.cli.commands;

import com.google.common.collect.Maps;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.UpdateProcessor;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.cobject.IndexUpdateRowKey;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 9/5/13
 */
public class UpdateCleaner extends RcliWithExistingKeyspace {

	public Options getCommandOptions(){
		Options ret = super.getCommandOptions();
		Option process = new Option( "p", "Process update fixes" );
		ret.addOption(process);

		OptionBuilder.withArgName("timeInNanos");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("List all updates in the system where the same object has been updated within timeInNanos");
		Option list = OptionBuilder.create( "listUpdates" );
		ret.addOption(list);

		OptionBuilder.withArgName("rowLimit");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("Limit the processer to only examine rowLimit rows");
		Option rowLimit = OptionBuilder.create( "rowLimit" );
		ret.addOption(rowLimit);

		return ret;
	}

	public boolean executeCommand(CommandLine cl){
		boolean ret = false;
		try {
			ret = super.executeCommand(cl);
		} catch (Exception e) {
			System.out.println("Exception executing command");
			e.printStackTrace();
		}
		if(!ret){
			return false;
		}
		try{
			getConnectionManager().setDefaultKeyspace(keyspaceDefinition);
			UpdateProcessor up = new UpdateProcessor(this.objectMapper);

			boolean didwork = false;
			long rowLimit = 0;
			if(cl.hasOption("rowLimit")) {
				rowLimit = Long.parseLong(cl.getOptionValue("rowLimit"));
			}
			if(cl.hasOption("listUpdates")){
				String timestr = cl.getOptionValue("listUpdates");
				long time = Long.parseLong(timestr);
				up.displayListResults(up.getUpdatesThatHappenedWithinTimeframe(time, rowLimit));
				didwork = true;
			}

			if(cl.hasOption("p")){
				up.process(rowLimit);
				didwork = true;
			}

			if(didwork){
				return true;
			} else {
				displayHelpMessage();
				return false;
			}
		}
		catch (IOException e){
			System.out.println("Error encountered processing updates: " + e.getMessage());
			return false;
		}

	}

}
