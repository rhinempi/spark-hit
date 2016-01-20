package com.cmg.sparkhit.io;

import com.cmg.sparkhit.util.InfoDumper;

import java.io.*;

/**
 * Created by Liren Huang on 13/01/16.
 * <p/>
 * SparkHit
 * <p/>
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * SparkHit is free software: you can redistribute it and/or modify it
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
public class ReadFasta implements InputFileManager {
    private String inputFasta;
    private InfoDumper info = new InfoDumper();

    public ReadFasta(String inputFasta){
        /**
         *
         */
        this.inputFasta = inputFasta;
    }

    /**
     *
     * @param inputFastaString
     * @return
     */
    public BufferedReader createInputFastaStream(String inputFastaString){
        BufferedReader fasta = null;
        try{
            FileInputStream inputFastaStream = new FileInputStream(inputFastaString);
            InputStreamReader inputFasta = new InputStreamReader(inputFastaStream);
            fasta = new BufferedReader(inputFasta);
        }catch (IOException e) {
            info.readIOException(e);
            info.screenDump();
            System.exit(0);
        }finally {
            return fasta;
        }
    }

    /**
     *
     * @param cFile
     */
    public void checkFile(String cFile){

        if (cFile.startsWith("s3")){
            info.readMessage("Input fasta file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using Map-Reduce FASTA reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input fasta file is lacated in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input fasta file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();

            File inputFasta = new File(cFile).getAbsoluteFile();
            if (!inputFasta.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                System.exit(0);
            }else {
                info.readMessage("Reading fasta file using BufferedReader");
                info.screenDump();
            }
        }
    }

    /**
     *
     * @param inputFile is the input text file in String
     */
    public void bufferInputFile(String inputFile) {
        checkFile(inputFile);
        BufferedReader fasta = createInputFastaStream(inputFile);
    }

    /**
     *
     * @param outputFile
     */
    public void bufferOutputFile(String outputFile){
        /**
         *
         */
    }
}
