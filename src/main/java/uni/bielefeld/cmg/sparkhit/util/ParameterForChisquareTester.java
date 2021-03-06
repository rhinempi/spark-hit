package uni.bielefeld.cmg.sparkhit.util;

/**
 * Created by rhinempi on 22.07.2017.
 *
 *       Sparkhit
 *
 * Copyright (c) 2017.
 *       Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an object for parsing the input options for Sparkhit-chisquaretester.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ParameterForChisquareTester {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public ParameterForChisquareTester(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_VCF = "vcf",
            INPUT_TAB = "tab",
            OUTPUT_LINE = "outfile",
            COLUMN = "column",
            COLUMN2 = "column2",
            CACHE = "cache",
            PARTITIONS = "partition",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();

    /**
     * This method places all input parameters into a hashMap.
     */
    public void putParameterID(){
        int o =0;

        parameterMap.put(INPUT_VCF, o++);
        parameterMap.put(INPUT_TAB, o++);
        parameterMap.put(OUTPUT_LINE, o++);
        parameterMap.put(COLUMN, o++);
        parameterMap.put(COLUMN2, o++);
        parameterMap.put(CACHE, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP, o++);
        parameterMap.put(HELP2, o++);
    }

    /**
     * This method adds descriptions to each parameter.
     */
    public void addParameterInfo(){


		/* use Object parameter of Options class to store parameter information */

        parameter.addOption(OptionBuilder.withArgName("input VCF file")
                .hasArg().withDescription("Input vcf file containing variation info")
                .create(INPUT_VCF));

        parameter.addOption(OptionBuilder.withArgName("input tabular file")
                .hasArg().withDescription("Input tabular file containing variation info")
                .create(INPUT_TAB));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output alleles p value")
                .create(OUTPUT_LINE));

        parameter.addOption(OptionBuilder.withArgName("Columns for Alleles")
                .hasArg().withDescription("1, columns where allele info is set, as case set")
                .create(COLUMN));

        parameter.addOption(OptionBuilder.withArgName("Columns2 for Alleles")
                .hasArg().withDescription("2, columns where allele info is set, as control set")
                .create(COLUMN2));

        parameter.addOption(OptionBuilder.withArgName("Cache data")
                .hasArg(false).withDescription("weather to cache data in memory or not, default no")
                .create(CACHE));

        parameter.addOption(OptionBuilder.withArgName("re-partition num")
                .hasArg().withDescription("even the load of each task, 1 partition for a task or 4 partitions for a task is recommended. Default, not re-partition")
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

    /**
     * This method parses input commandline arguments and sets correspond
     * parameters.
     *
     * @return {@link DefaultParam}.
     */
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
                help.printStatisticerHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printStatisticerHelp();
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

            if ((value = cl.getOptionValue(COLUMN)) != null){
                param.columns = value;
                param.columnStart = Integer.decode(value.split("-")[0]);
                param.columnEnd = Integer.decode(value.split("-")[1]);
            }else{
                param.columnStart = Integer.decode(param.columns.split("-")[0]);
                param.columnEnd = Integer.decode(param.columns.split("-")[1]);
            }

            if ((value = cl.getOptionValue(COLUMN2)) != null){
                param.columns2 = value;
                param.column2Start = Integer.decode(value.split("-")[0]);
                param.column2End = Integer.decode(value.split("-")[1]);
            }else{
                param.column2Start = Integer.decode(param.columns.split("-")[0]);
                param.column2End = Integer.decode(param.columns.split("-")[1]);
            }

            if (cl.hasOption(CACHE)){
                param.cache =true;
            }

            if ((value = cl.getOptionValue(INPUT_VCF)) != null) {
                param.inputFqPath = value;
            }else if ((value = cl.getOptionValue(INPUT_TAB)) != null) {
                param.inputFqPath = value;
                param.inputTabPath = value;
            }else {
                help.printStatisticerHelp();
                System.exit(0);
//                throw new IOException("Input file not specified.\nUse -help for list of options");
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
                help.printStatisticerHelp();
                info.readMessage("Output file not set with -outfile options");
                info.screenDump();
                System.exit(0);
            }


            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()){
                info.readParagraphedMessages("Output file : \n\t" + param.outputPath + "\nalready exists, will be overwrite.");
                info.screenDump();
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }


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