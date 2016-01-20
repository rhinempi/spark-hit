package com.cmg.sparkhit.util;

/**
 * Created by Liren Huang on 13/01/16.
 * <p/>
 * FragRec
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * FragRec is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 * <p/>
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.err;

public class Parameter {
    private String[] arguments;

    public Parameter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String INPUT_FASTQ = "fastq", INPUT_REF = "reference", OUTPUT_FILE = "outfile", KMER_SIZE = "kmer", EVALUE = "evalue", UNMASK = "unmask", OVERLAP = "overlap", IDENTITY = "identity", COVERAGE = "coverage", MINLENGTH = "minlength", ATTEMPTS = "attempts", HITS = "hits", STRAND = "strand", THREADS = "thread", VERSION = "version", HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();


    public void putParameterID(){
        int o =0;

        parameterMap.put(INPUT_FASTQ, o++);
        parameterMap.put(INPUT_REF, o++);
        parameterMap.put(OUTPUT_FILE, o++);
        parameterMap.put(KMER_SIZE, o++);
        parameterMap.put(EVALUE, o++);
        parameterMap.put(UNMASK, o++);
        parameterMap.put(OVERLAP, o++);
        parameterMap.put(IDENTITY, o++);
        parameterMap.put(COVERAGE, o++);
        parameterMap.put(MINLENGTH, o++);
        parameterMap.put(ATTEMPTS, o++);
        parameterMap.put(HITS, o++);
        parameterMap.put(STRAND, o++);
        parameterMap.put(THREADS, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP, o++);
    }

    public void addParameterInfo(){

		/* use Object parameter of Options class to store parameter information */
        parameter.addOption(OptionBuilder.withArgName("input fastq file")
                .hasArg().withDescription("Input Next Generation Sequencing (NGS) data, usually fastq format file, as input file")
                .create(INPUT_FASTQ));

        parameter.addOption(OptionBuilder.withArgName("input reference")
                .hasArg().withDescription("Input genome reference file, usually fasta format file, as input file")
                .create(INPUT_REF));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output fragment recruitment file in text format")
                .create(OUTPUT_FILE));

        parameter.addOption(OptionBuilder.withArgName("kmer size")
                .hasArg().withDescription("Kmer length for fragment recruitment")
                .create(KMER_SIZE));

        parameter.addOption(OptionBuilder.withArgName("e-value")
                .hasArg().withDescription("e-value threshold, default 10")
                .create(EVALUE));

        parameter.addOption(OptionBuilder.withArgName("unmask")
                .hasArg().withDescription("whether mask repeats of lower case nucleotides: 1: yes; 0 :no; default=1")
                .create(UNMASK));

        parameter.addOption(OptionBuilder.withArgName("kmer overlap")
                .hasArg().withDescription("small overlap for long read")
                .create(OVERLAP));

        parameter.addOption(OptionBuilder.withArgName("identity threshold")
                .hasArg().withDescription("minimal identity for recruiting a read, default 75")
                .create(IDENTITY));

        parameter.addOption(OptionBuilder.withArgName("coverage threshold")
                .hasArg().withDescription("minimal coverage for recruiting a read, default 30")
                .create(COVERAGE));

        parameter.addOption(OptionBuilder.withArgName("minimal read length")
                .hasArg().withDescription("minimal read length required for processing")
                .create(MINLENGTH));

        parameter.addOption(OptionBuilder.withArgName("number attempts")
                .hasArg().withDescription("maximum number of alignment attempts for one read to a block, default 20")
                .create(ATTEMPTS));

        parameter.addOption(OptionBuilder.withArgName("hit number")
                .hasArg().withDescription("how many hits for output: 0:all; N: top N hits")
                .create(HITS));

        parameter.addOption(OptionBuilder.withArgName("strand +/-")
                .hasArg().withDescription("")
                .create(STRAND));

        parameter.addOption(OptionBuilder.withArgName("number of threads")
                .hasArg().withDescription("How many threads to use for parallelizing processes," + "default is the number of cpus available!" + "local mode only, for Spark version, use spark parameter!")
                .create(THREADS));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("show version information")
                .create(VERSION));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("print and show this information")
                .create(HELP));
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
                help.printHelp();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)){
               System.exit(0);
            }

			/* Checking all parameters */
            String value;
            if ((value = cl.getOptionValue(THREADS)) != null){
                if (Integer.decode(value) <= threads){
                    threads = Integer.decode(value);
                }else if (Integer.decode(value) < 0){
                    throw new RuntimeException("Parameter " + THREADS +
                        " come on, CPU number could not be smaller than 1");
                }else{
                    throw new RuntimeException("Parameter " + THREADS +
                        " is bigger than the number of your CPUs. Should be smaller than " + threads);
                }
            }

            if ((value = cl.getOptionValue(KMER_SIZE)) != null){
                if (Integer.decode(value) >= 8 || Integer.decode(value) <= 12){
                    param.kmerSize = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + KMER_SIZE +
                        " should be set between 8-12");
                }
            }

            if ((value = cl.getOptionValue(EVALUE)) != null){
                param.eValue = Double.parseDouble(value);
            }

            if ((value = cl.getOptionValue(UNMASK)) != null){
                if (Integer.decode(value) == 1 || Integer.decode(value) == 0){
                    param.maskRepeat = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + UNMASK +
                        " should be set as 1 or 0");
                }
            }

            if ((value = cl.getOptionValue(OVERLAP)) != null){
                if (Integer.decode(value) >= 0 || Integer.decode(value) <= param.kmerSize){
                    param.kmerOverlap = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + OVERLAP +
                        " should not be bigger than kmer size or smaller than 0");
                }
            }

            if ((value = cl.getOptionValue(IDENTITY)) != null){
                if (Integer.decode(value) >= 0 || Integer.decode(value) <= 100){
                    param.readIdentity = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + IDENTITY +
                        " should not be integer of %");
                }
            }

            if ((value = cl.getOptionValue(COVERAGE)) != null){
                param.alignLength = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(MINLENGTH)) != null){
                if (Integer.decode(value) >= 0 ){
                    param.minReadSize = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + MINLENGTH +
                        " should be larger than 0");
                }
            }

            if ((value = cl.getOptionValue(ATTEMPTS)) != null){
                if (Integer.decode(value) >= 1 ){
                    param.maxTrys = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + ATTEMPTS +
                        " at least try once");
                }
            }

            if ((value = cl.getOptionValue(HITS)) != null){
                if (Integer.decode(value) >= 0){
                    param.reportRepeatHits = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + HITS +
                        " should be bigger than 0");
                }
            }

            if ((value = cl.getOptionValue(STRAND)) != null){
                if (Integer.decode(value) == 0 || Integer.decode(value) == 1 || Integer.decode(value) == 2){
                    param.chains = Integer.decode(value);
                }else{
                    throw new RuntimeException("Parameter " + STRAND +
                        " should be either 0, 1 or 2");
                }
            }

            if ((value = cl.getOptionValue(INPUT_FASTQ)) != null){
                param.inputFqPath = value;
            }else{
                throw new IOException("Input query file not specified.\nUse -help for list of options");
            }

			/* not applicable for HDFS and S3 */
//			File inputFastq = new File(param.inputFqPath).getAbsoluteFile();
//			if (!inputFastq.exists()){
//				err.println("Input query file not found.");
//				return;
//			}

            if ((value = cl.getOptionValue(INPUT_REF)) != null){
                param.inputFaPath = value;
            }else{
                err.println("Input reference file not specified.");
            }
            File inputFasta = new File(param.inputFaPath).getAbsoluteFile();
            if (!inputFasta.exists()){
                err.println("Input reference file not found.");
            }

            if ((value = cl.getOptionValue(OUTPUT_FILE)) != null){
                param.outputPath = value;
            }
            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()){
                System.out.println("Output file " + param.outputPath + "already exists, will be override.");
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }

            if (param.inputFqPath.endsWith(".gz")){
            }
            if (param.inputFaPath.endsWith(".gz")){
            }

        } catch (IOException e) { // Don`t catch this, NaNaNaNa, U can`t touch this.
            err.println("Parameter setting incorrect.");
            err.println();
            e.printStackTrace();
            System.exit(0);
        } catch (RuntimeException e){
            err.println("Parameter setting incorrect.");
            err.println();
            e.printStackTrace();
            System.exit(0);
        } catch (ParseException e){
            err.println("Parameter setting incorrect.");
            err.println();
            e.printStackTrace();
            System.exit(0);
        }

    return param;
    }
}