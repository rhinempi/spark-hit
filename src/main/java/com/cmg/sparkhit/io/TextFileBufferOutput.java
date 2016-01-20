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
public class TextFileBufferOutput implements OutputFileManager {
    private String path;
    private InfoDumper info = new InfoDumper();
    private File outputPath;
    private FileOutputStream outputFileStream;
    private OutputStreamWriter outputStreamWriter;
    public BufferedWriter outputBufferWriter;

    public TextFileBufferOutput(){
        /**
         *
         */
    }

    /**
     *
     * @return return BufferWriter for output text
     */
    public BufferedWriter getBufferWriter(){
        return outputBufferWriter;
    }

    /**
     *
     * set class BufferWriter object outputBufferWriter
     */
    private void setBufferWriter(){
        this.outputBufferWriter = new BufferedWriter(outputStreamWriter);
    }

    /**
     *
     * set class OutputStreamWriter object outputStreamWriter
     */
    private void setOutputStreamWriter(){
        this.outputStreamWriter = new OutputStreamWriter(outputFileStream);
        setBufferWriter();
    }

    /**
     * set class FileOutputStream object outputFileStream
     *
     * @param overwrite stands for whether overwrite the log file or not
     */
    private void setFileOutputStream(boolean overwrite){
        try{
            this.outputFileStream = new FileOutputStream(outputPath, overwrite);
            setOutputStreamWriter();
        }catch(FileNotFoundException e){
            info.readFileNotFoundException(e);
            info.screenDump();
            System.exit(0);
        }
    }

    /**
     * set class File object outputPath
     *
     * @param overwrite
     */
    private void setOutputFile(boolean overwrite){
        this.outputPath = new File(path);
        setFileOutputStream(overwrite);
    }

    /**
     *
     * @param outputFile out put file path
     * @param overwrite  whether overwrite existing file
     */
    public void setOutput(String outputFile, boolean overwrite){
        this.path = outputFile;
        setOutputFile(overwrite);
    }

    /**
     * Override interface method
     *
     * @param outputFile is the out put file path in String.
     */
    public void bufferOutputFile(String outputFile){
        this.path = outputFile;
    }

    /**
     *
     * @param inputFile
     */
    public void bufferInputFile(String inputFile){
        /**
         *   this extended method is invalid
         */
    }
}
