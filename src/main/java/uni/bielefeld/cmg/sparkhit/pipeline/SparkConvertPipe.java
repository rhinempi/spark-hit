package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.zookeeper.version.Info;
import scala.Tuple2;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;

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

/**
 * Returns an object for running the Sparkhit converter pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkConvertPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    /**
     * runs the Sparkhit pipeline using Spark RDD operations.
     */
    public void spark() {
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> FastqRDD = sc.textFile(param.inputFqPath);

        class FastqFilter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
 //                   if (s.startsWith("@")){
                        return true;
//                    }else{
//                        return false;
//                    }
                }else{
                    return false;
                }
            }
        }

        class FastqConcat implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the fastq file.
             * @return the concatenated fastq unit (one line).
             */
            public String call(String s){
                if (s.startsWith("@")){
                    line = s;
                    lineMark = 1;
                    return null;
                }else if (lineMark == 1){
                    line = line + "\t" + s;
                    lineMark = 2;
                    return line;
                }else{
                    lineMark = 2;
                    return null;
                }
            }
        }

        class FastqConcatWithQual implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the fastq file.
             * @return the concatenated fastq unit (four lines).
             */
            public String call(String s) {
                if (lineMark == 2) {
                    lineMark++;
                    line = line + "\t" + s;
                    return null;
                } else if (lineMark == 3) {
                    lineMark++;
                    line = line + "\t" + s;
                    return line;
                } else if (s.startsWith("@")) {
                    line = s;
                    lineMark = 1;
                    return null;
                } else if (lineMark == 1) {
                    line = line + "\t" + s;
                    lineMark++;
                    return null;
                }else{
                    return null;
                }
            }
        }

        class FastqConcatToFasta implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the fastq file.
             * @return the transformed fasta unit.
             */
            public String call(String s){
                if (s.startsWith("@")){
                    line = s;
                    lineMark = 1;
                    return null;
                }else if (lineMark == 1){
                    line = ">" + line + "\n" + s;
                    lineMark = 2;
                    return line;
                }else{
                    lineMark = 2;
                    return null;
                }
            }
        }

        if (param.outputformat == 0) {      // fastq to line without quality
            FastqConcat RDDConcat = new FastqConcat();
            FastqRDD = FastqRDD.map(RDDConcat);
        }else if (param.outputformat == 1){ // fastq to line with quality
            FastqConcatWithQual RDDConcatQ = new FastqConcatWithQual();
            FastqRDD = FastqRDD.map(RDDConcatQ);
        }else {                             // fastq to fasta file
            FastqConcatToFasta RDDConcatToFasta = new FastqConcatToFasta();
            FastqRDD = FastqRDD.map(RDDConcatToFasta);
        }

        FastqFilter RDDFilter = new FastqFilter();
        FastqRDD = FastqRDD.filter(RDDFilter);

        long readNumber = FastqRDD.count();

        info.readMessage("Total read number: " + readNumber);
        info.screenDump();

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }

        FastqRDD.saveAsTextFile(param.outputPath);

        sc.stop();
    }

    /**
     * This method sets the input parameters.
     *
     * @param param
     */
    public void setParam(DefaultParam param){
        this.param = param;
    }
}
