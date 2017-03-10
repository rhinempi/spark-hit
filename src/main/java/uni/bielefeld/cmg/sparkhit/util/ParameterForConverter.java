package uni.bielefeld.cmg.sparkhit.util;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ParameterForConverter {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    public ParameterForConverter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_FASTQ = "fastq",
            OUTPUT_LINE = "outfile",
            OUTPUT_FORMAT = "outfm",
            PARTITIONS = "partition",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();


    public void putParameterID(){
        int o =0;

        parameterMap.put(INPUT_FASTQ, o++);
        parameterMap.put(OUTPUT_LINE, o++);
        parameterMap.put(OUTPUT_FORMAT, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP, o++);
        parameterMap.put(HELP2, o++);
    }

    public void addParameterInfo(){


		/* use Object parameter of Options class to store parameter information */

        parameter.addOption(OptionBuilder.withArgName("input fastq file")
                .hasArg().withDescription("Input Next Generation Sequencing (NGS) data, fastq file format, four line per unit")
                .create(INPUT_FASTQ));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output sequencing data directory")
                .create(OUTPUT_LINE));

        parameter.addOption(OptionBuilder.withArgName("output file format")
                .hasArg().withDescription("Output sequencing data format: 0 line without quality, 1 line with quality, 2 fasta format")
                .create(OUTPUT_FORMAT));

        parameter.addOption(OptionBuilder.withArgName("re-partition number")
                .hasArg().withDescription("re generate number of partitions for .gz data, as .gz data only have one partition (spark parallelization)")
                .create(PARTITIONS));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("show version information")
                .create(VERSION));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("print and show this information")
                .create(HELP));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("")
                .create(HELP2));

    }

    /* main method */
    public DefaultParam importCommandLine() {

        /* Assigning Parameter ID to an ascending number */
        putParameterID();

        /* Assigning parameter descriptions to each parameter ID */
        addParameterInfo();

        /* need a Object parser of PosixParser class for the function parse of CommandLine class */
        PosixParser parser = new PosixParser();

        /* print out help information */
        HelpParam help = new HelpParam(parameter, parameterMap);

        /* check each parameter for assignment */
        try {
            long input_limit = -1;
            int threads = Runtime.getRuntime().availableProcessors();

			/* Set Object cl of CommandLine class for Parameter storage */
            CommandLine cl = parser.parse(parameter, arguments, true);
            if (cl.hasOption(HELP)) {
                help.printConverterHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printConverterHelp();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)){
               System.exit(0);
            }

			/* Checking all parameters */

            String value;

            if ((value = cl.getOptionValue(PARTITIONS)) != null){
                param.partitions = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(INPUT_FASTQ)) != null) {
                param.inputFqPath = value;
            }else {
                help.printConverterHelp();
                System.exit(0);
//                throw new IOException("Input file not specified.\nUse -help for list of options");
            }

            if ((value = cl.getOptionValue(OUTPUT_FORMAT)) != null){
                param.outputformat = Integer.decode(value);
            }

			/* not applicable for HDFS and S3 */
            /* using TextFileBufferInput for such purpose */
//			File inputFastq = new File(param.inputFqPath).getAbsoluteFile();
//			if (!inputFastq.exists()){
//				err.println("Input query file not found.");
//				return;
//i			}

            if ((value = cl.getOptionValue(OUTPUT_LINE)) != null){
                param.outputPath = value;
            }else{
                info.readMessage("Output file not set of -outfile options");
                info.screenDump();
            }


            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()){
                info.readParagraphedMessages("Output file : \n\t" + param.outputPath + "\nalready exists, will be overwrite.");
                info.screenDump();
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }

           // param.bestNas = (param.alignLength * param.readIdentity) / 100;
           // param.bestKmers = param.alignLength - (param.alignLength - param.bestNas) * 4 - 3;

        } catch (IOException e) { // Don`t catch this, NaNaNaNa, U can`t touch this.
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (RuntimeException e){
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (ParseException e){
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        }

        return param;
    }
}