package com.pardot.rhombus.cli.commands;

import com.google.common.collect.Maps;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CQLGenerationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.util.Map;

/**
 * User: Rob Righter
 * Date: 8/19/13
 */
public class SetCompaction extends RcliWithExistingKeyspace {

    public Options getCommandOptions(){
        Options ret = super.getCommandOptions();
        Option strategy = OptionBuilder.withArgName("name")
                .hasArg()
                .withDescription("Compaction Strategy Name (LeveledCompactionStrategy or SizeTieredCompactionStrategy) ")
                .create( "strategy" );
        Option sstableSize = OptionBuilder.withArgName("size")
                .hasArg()
                .withDescription("SSTable size in mb (for LeveledCompactionStrategy) ")
                .create( "sstableSize" );
        Option minThreshold = OptionBuilder.withArgName("size")
                .hasArg()
                .withDescription("minimum number of SSTables to trigger a minor compaction (for SizeTieredCompactionStrategy) ")
                .create( "minThreshold" );
        ret.addOption(strategy);
        ret.addOption(sstableSize);
        ret.addOption(minThreshold);
        return ret;
    }

    public void executeCommand(CommandLine cl){
        super.executeCommand(cl);

        try{
            getConnectionManager().setDefaultKeyspace(keyspaceDefinition);
            String strategy = cl.getOptionValue("strategy");
            Map<String,Object> options = Maps.newHashMap();
            if(cl.hasOption("sstableSize")){
                options.put("sstable_size_in_mb", Integer.parseInt(cl.getOptionValue("sstableSize")));
            }
            if(cl.hasOption("minThreshold")){
                options.put("min_threshold", Integer.parseInt(cl.getOptionValue("minThreshold")));
            }
            this.objectMapper.setCompaction(strategy,options);
            //looks like a success. Lets go ahead and return as such
            System.exit(0);
        }
        catch (CQLGenerationException e){
            System.out.println("Error encountered setting compaction: " + e.getMessage());
        } catch (RhombusException e) {
			System.out.println("Error encountered setting compaction: " + e.getMessage());
		}

	}

}
