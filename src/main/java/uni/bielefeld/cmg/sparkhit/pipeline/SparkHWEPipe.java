package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import uni.bielefeld.cmg.sparkhit.algorithm.Statistic;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;

/**
 * Created by Liren Huang on 17/03/16.
 * <p/>
 * SparkHit
 * <p/>
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * <p/>
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 */


public class SparkHWEPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    public void spark() {
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> vcfRDD = sc.textFile(param.inputFqPath);

        class VariantToPValue implements Function<String, String> {
            public String call(String s) {

                double pHWE;

                if (s.startsWith("#")) {
                    return null;
                }

                String[] array = s.split("\\t");

                if (array.length <= 9) {
                    return null;
                }

                int p2 = 0, q2 = 0, pq = 0;
                for (int i = 9; i < 9 + 1000; i++) {
                    if (array[i].equals("0|0")) {
                        p2++;
                    } else if (array[i].equals("0|1") || array[i].equals("1|0")) {
                        pq++;
                    } else if (array[i].equals("1|1")) {
                        q2++;
                    }
                }
                pHWE = Statistic.calculateExactHWEPValue(pq, p2, q2);
                return array[0] + "\t" + array[1] + "\t" + pHWE;
            }
        }

        class Filter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
                    return true;
                }else{
                    return false;
                }
            }
        }

        if (param.partitions != 0) {
            vcfRDD = vcfRDD.repartition(param.partitions);
        }

        VariantToPValue toPValue = new VariantToPValue();
        JavaRDD<String> pValueRDD = vcfRDD.map(toPValue);

        Filter RDDFilter = new Filter();
        pValueRDD = pValueRDD.filter(RDDFilter);

        pValueRDD.saveAsTextFile(param.outputPath);

    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
