package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.reference.RefStructSerializer;
import uni.bielefeld.cmg.sparkhit.struct.BinaryBlock;
import uni.bielefeld.cmg.sparkhit.struct.KmerLoc;
import uni.bielefeld.cmg.sparkhit.struct.RefTitle;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;
import java.util.Iterator;
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
//    private RefStructBuilder ref;
  //  private ScoreMatrix mat;

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
        conf.set("spark.kryo.referenceTracking", "false");
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    private RefStructBuilder buildReference(){
        info.readMessage("start building reference index.");
        info.screenDump();
        RefStructBuilder ref = new RefStructBuilder();

        info.readMessage("parsing parameters ...");
        info.screenDump();
        ref.setParameter(param);
        info.readMessage("kmer size : " + param.kmerSize);
        info.screenDump();

        info.readMessage("start loading reference sequence.");
        info.screenDump();
        clockStart();
        ref.loadRef(param.inputFaPath);
        long T = clockCut();
        info.readParagraphedMessages("loaded " + ref.totalLength + " bases, " + ref.totalNum + " contigs.\ntook " + T + " ms.");
        info.screenDump();

        info.readMessage("start building reference index.");
        info.screenDump();
        clockStart();
        ref.buildIndex();
        T = clockCut();
        info.readMessage("took " + T + " ms.");
        info.screenDump();

        return ref;
    }

    public RefStructBuilder loadReference(){
        RefStructSerializer refSer = new RefStructSerializer();
        refSer.setParameter(param);

        clockStart();
        refSer.javaDeSerialization();
        long T = clockCut();
        info.readParagraphedMessages("loaded reference genome index.\ntook " + T + " ms.");
        info.screenDump();
        RefStructBuilder ref = refSer.getStruct();

        return ref;
    }

    public void sparkFastqToLine(){
        SparkConf conf = setSparkConfiguration();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> FastqRDD = sc.textFile(param.inputFqPath);

        class FastqFilter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
          //          if (s.startsWith("@")){
                        return true;
            //        }else{
            //            return false;
            //        }
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

        FastqConcat RDDConcat = new FastqConcat();
        FastqRDD = FastqRDD.map(RDDConcat);

        FastqFilter RDDFilter = new FastqFilter();
        FastqRDD = FastqRDD.filter(RDDFilter);

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }

        FastqRDD.saveAsTextFile(param.outputPath);
    }

    /**
     * reading line based fastq data
     */
    public void sparkLineFile(){
        SparkConf conf = setSparkConfiguration();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> FastqRDD;

        if (param.filename){
            class Tuple2String implements Function<Tuple2<String, String>, String>, Serializable{
                public String call(Tuple2<String, String> s){
                    return ("@" + s._1 + "|" + s._2);
                }
            }

            Tuple2String RDDmerge = new Tuple2String();

            JavaPairRDD<LongWritable, Text> javaPairRDD = sc.newAPIHadoopFile(
                    param.inputFqPath,
                    TextInputFormat.class,
                    LongWritable.class,
                    Text.class,
                    new Configuration()
            );

            JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;

            JavaRDD<Tuple2<String, String>> namedLinesRDD = hadoopRDD.mapPartitionsWithInputSplit(
                    new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>() {
                        @Override
                        public Iterator<Tuple2<String, String>> call(InputSplit inputSplit, final Iterator<Tuple2<LongWritable, Text>> lines) throws Exception {
                            FileSplit fileSplit = (FileSplit) inputSplit;
                            final String fileName = fileSplit.getPath().getName();
                            return new Iterator<Tuple2<String, String>>() {
                                @Override
                                public boolean hasNext() {
                                    return lines.hasNext();
                                }
                                @Override
                                public Tuple2<String, String> next() {
                                    Tuple2<LongWritable, Text> entry = lines.next();
                                    return new Tuple2<String, String>(fileName, entry._2().toString());
                                }

                                @Override
                                public void remove() {
                                    throw new IllegalStateException();
                                }
                            };
                        }
                    },
                    true
            );

            FastqRDD = namedLinesRDD.map(RDDmerge);
        }else {

            FastqRDD = sc.textFile(param.inputFqPath);

        }

        RefStructBuilder ref = buildReference();

        ScoreMatrix mat = new ScoreMatrix();

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



        class SparkBatchAlign implements FlatMapFunction<String, String>, Serializable{

            BatchAlignPipe bPipe = new BatchAlignPipe(broadParam.value());

            public Iterator<String> call(String s){

                bPipe.BBList = broadBBList.value();
                bPipe.index = broadIndex.value();
                bPipe.listTitle = broadListTitle.value();
                bPipe.mat = broadMat.value();
                bPipe.totalLength = totalLength;
                bPipe.totalNum = totalNum;

                return bPipe.sparkRecruit(s).iterator();
            }
        }

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }

        SparkBatchAlign RDDBatch = new SparkBatchAlign();
        FastqRDD = FastqRDD.flatMap(RDDBatch);

        FastqRDD.saveAsTextFile(param.outputPath);
    }

    /**
     *
     */
    public void spark(){
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> FastqRDD;

        if (param.filename){
            class Tuple2String implements Function<Tuple2<String, String>, String>, Serializable{
                public String call(Tuple2<String, String> s){
                    if (s._2.startsWith("@")) {
                        return ("@" + s._1 + "|" + s._2);
                    }else{
                        return s._2;
                    }
                }
            }

            Tuple2String RDDmerge = new Tuple2String();

            JavaPairRDD<LongWritable, Text> javaPairRDD = sc.newAPIHadoopFile(
                    param.inputFqPath,
                    TextInputFormat.class,
                    LongWritable.class,
                    Text.class,
                    new Configuration()
            );

            JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;

            JavaRDD<Tuple2<String, String>> namedLinesRDD = hadoopRDD.mapPartitionsWithInputSplit(
                    new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>() {
                        @Override
                        public Iterator<Tuple2<String, String>> call(InputSplit inputSplit, final Iterator<Tuple2<LongWritable, Text>> lines) throws Exception {
                            FileSplit fileSplit = (FileSplit) inputSplit;
                            final String fileName = fileSplit.getPath().getName();
                            return new Iterator<Tuple2<String, String>>() {
                                @Override
                                public boolean hasNext() {
                                    return lines.hasNext();
                                }
                                @Override
                                public Tuple2<String, String> next() {
                                    Tuple2<LongWritable, Text> entry = lines.next();
                                    return new Tuple2<String, String>(fileName, entry._2().toString());
                                }

                                @Override
                                public void remove() {
                                    throw new IllegalStateException();
                                }
                            };
                        }
                    },
                    true
            );

            FastqRDD = namedLinesRDD.map(RDDmerge);
        }else {

            FastqRDD = sc.textFile(param.inputFqPath);
        }

        RefStructBuilder ref = buildReference();

        ScoreMatrix mat = new ScoreMatrix();

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
//                    if (s.startsWith("@")){
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

            public Iterator<String> call(String s){

                bPipe.BBList = broadBBList.value();
                bPipe.index = broadIndex.value();
                bPipe.listTitle = broadListTitle.value();
                bPipe.mat = broadMat.value();
                bPipe.totalLength = totalLength;
                bPipe.totalNum = totalNum;

                return bPipe.sparkRecruit(s).iterator();
            }
        }

        /**
         * transformation operation of spark
         */
        FastqConcat RDDConcat = new FastqConcat();
        FastqRDD = FastqRDD.map(RDDConcat);

        FastqFilter RDDFilter = new FastqFilter();
        FastqRDD = FastqRDD.filter(RDDFilter);

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }

        SparkBatchAlign RDDBatch = new SparkBatchAlign();
        FastqRDD = FastqRDD.flatMap(RDDBatch);

        /**
         * action operation of spark
         */
        FastqRDD.saveAsTextFile(param.outputPath);
        sc.stop();
    }

    public void setParam(DefaultParam param){
        this.param = param;
    }

    public void setStruct(RefStructBuilder ref) {
    //    this.ref = ref;
    }

    public void setMatrix(ScoreMatrix mat){
      //  this.mat = mat;
    }
}

