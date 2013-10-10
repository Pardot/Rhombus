package com.pardot.rhombus.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Created with IntelliJ IDEA.
 * User: rrighter
 * Date: 8/17/13
 * Time: 11:38 AM
 * To change this template use File | Settings | File Templates.
 */
public interface RhombusCommand {
    public Options getCommandOptions();
    public boolean executeCommand(CommandLine cl);
}
