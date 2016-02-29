package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.struct.*;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liren Huang on 29/02/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 */


public class SparkPipe implements Serializable {
    private long time;
    private DefaultParam param;
    private RefStructBuilder ref;
    private ScoreMatrix mat;

    private InfoDumper info = new InfoDumper();

    private void clockStart(){
        time = System.currentTimeMillis();
    }

    private long clockCut(){
        long tmp = time;
        time = System.currentTimeMillis();
        return time - tmp;
    }

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    public void spark(){
        SparkConf conf = setSparkConfiguration();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> FastqRDD = sc.textFile(param.inputFqPath);

        clockStart();
        final Broadcast<List<BinaryBlock>> broadBBList = sc.broadcast(ref.BBList);
        final Broadcast<List<RefTitle>> broadListTitle = sc.broadcast(ref.title);
        final Broadcast<KmerLoc[]> broadIndex = sc.broadcast(ref.index);
        final Broadcast<DefaultParam> broadParam = sc.broadcast(param);
        final Broadcast<ScoreMatrix> broadMat = sc.broadcast(mat);
        final long totalLength = ref.totalLength; // not broadcasting
        final int totalNum = ref.totalNum; // not broadcasting
        long T = clockCut();

        info.readMessage("Spark kryo reference data structure serialization time : "  + T + " ms");
        info.screenDump();

        class FastqFilter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
                    if (s.startsWith("@")){
                        return true;
                    }else{
                        return false;
                    }
                }else{
                    return false;
                }
            }
        }

        class FastqConcat implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
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
                    lineMark++;
                    return null;
                }
            }
        }

        class SparkBatchAlign implements FlatMapFunction<String, String>, Serializable{
            BatchAlignPipe bPipe = new BatchAlignPipe(broadParam.value());
            public Iterable<String> call(String s){

                bPipe.BBList = broadBBList.value();
                bPipe.index = broadIndex.value();
                bPipe.listTitle = broadListTitle.value();
                bPipe.mat = broadMat.value();
                bPipe.totalLength = totalLength;
                bPipe.totalNum = totalNum;

                return bPipe.sparkRecruit(s);
            }
        }

        /**
         * transformation operation of spark
         */
        FastqConcat RDDConcat = new FastqConcat();
        JavaRDD<String> FastqRDD2 = FastqRDD.map(RDDConcat);

        FastqFilter RDDFilter = new FastqFilter();
        JavaRDD<String> FastqRDD3 = FastqRDD2.filter(RDDFilter);

        JavaRDD<String> FastqRDD4 = FastqRDD3.repartition(param.threads);

        SparkBatchAlign RDDBatch = new SparkBatchAlign();
        JavaRDD<String> FastqRDD5 = FastqRDD4.flatMap(RDDBatch);

        /**
         * action operation of spark
         */
        FastqRDD5.saveAsTextFile(param.outputPath);
    }

    public void setParam(DefaultParam param){
        this.param = param;
    }

    public void setStruct(RefStructBuilder ref) {
        this.ref = ref;
    }

    public void setMatrix(ScoreMatrix mat){
        this.mat = mat;
    }
}

