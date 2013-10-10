package com.pardot.rhombus.cli;


import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.apache.commons.cli.*;

import java.io.IOException;

/**
 * User: Rob Righter
 * Date: 8/17/13
 * Time: 11:06 AM
 */
public class RhombusCli implements  RhombusCommand {

    public CKeyspaceDefinition keyspaceDefinition;

    public static Options makeBootstrapOptions(){
        Options ret = new Options();
        Option help = new Option( "help", "print this message" );
        ret.addOption(help);
        return ret;

    }

    public Options getCommandOptions(){
        Options ret = makeBootstrapOptions();
        return ret;
    }

    public void executeCommand(CommandLine cl){


    }

    public void displayHelpMessageAndExit(){
        HelpFormatter formatter = new HelpFormatter();
        String cmdName = this.getClass().getName().replaceAll("com.pardot.rhombus.cli.commands.","");
        formatter.printHelp( "RhombusCli "+cmdName, getCommandOptions());
        System.exit(1);
    }

    public static void main( String[] args ) {
        // create the parser
        CommandLineParser parser = new BasicParser();
        try {
            // make sure they gave us a command
            if( args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "RhombusCli", makeBootstrapOptions() );
                System.exit(1);
            }
            //Load up the class
            //if the class name is not fully qualified we assume its in com.pardot.rhombus.cli.commands
            String className = args[0];
            if(!className.contains(".")){
                className = "com.pardot.rhombus.cli.commands."+ className;
            }

            try{
                RhombusCommand cmd = (RhombusCommand)(Class.forName(className)).newInstance();
                Options commandOptions = cmd.getCommandOptions();
                cmd.executeCommand(parser.parse( commandOptions, args ));
            }
            catch (ClassNotFoundException e){
                System.out.println("Could not find Command Class "+className);
            }
            catch (IllegalAccessException e){
                System.out.println("Could not access Command Class "+className);
            }
            catch (InstantiationException e){
                System.out.println("Could not instantiate Command Class "+className);
            }
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
    }


}
