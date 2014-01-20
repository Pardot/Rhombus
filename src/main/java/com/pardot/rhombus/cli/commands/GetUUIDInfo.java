package com.pardot.rhombus.cli.commands;

import com.pardot.rhombus.cli.RhombusCli;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.util.UuidUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.util.UUID;

/**
 * User: Rob Righter
 * Date: 10/31/13
 */
public class GetUUIDInfo extends RhombusCli {

	public Options getCommandOptions(){
		Options ret = super.getCommandOptions();
		Option uuid = OptionBuilder.withArgName("uuidAsString")
				.hasArg()
				.withDescription("String version of the UUID.")
				.create( "uuid" );
		ret.addOption(uuid);
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
		if(!cl.hasOption("uuid")){
			displayHelpMessage();
			return false;
		}

		String stringuuid = cl.getOptionValue("uuid");
		UUID uuid = UUID.fromString(stringuuid);
		Integer namespace = UuidUtil.namespaceFromUUID(uuid);
		Integer name = UuidUtil.nameFromUUID(uuid);

		System.out.println("Namespace = " + namespace);
		System.out.println("Name = " + name);

		return true;
	}

}
