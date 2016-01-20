package com.cmg.sparkhit.serializer;

import com.cmg.sparkhit.io.ObjectFileOutput;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.FileOutputStream;

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
public class kryoSerializer implements ShKryoSerializer {

    private Kryo kryo = new Kryo();
    private Output output;

    /**
     *
     * @param object
     * @param outFile
     */
    public void javaSerialization(Object object, String outFile){

    }

    /**
     *
     * @param inFile
     * @return
     */
    public Object javaDeSerialization(String inFile){
        return null;
    }

    public void setRegisterClass(Class myClass){
        kryo.register(myClass);
    }

    public void writeObject(FileOutputStream fileOut, Object object){
        output = new Output(fileOut);
        kryo.writeObject(output, object);
    }

    public void kryoSerialization(Object object, String outFile){
        Class myClass = object.getClass();
        setRegisterClass(myClass);

        ObjectFileOutput outputObject = new ObjectFileOutput();
        outputObject.setOutput(outFile, false);
        FileOutputStream fileOut = outputObject.getFileOutputStream();

        writeObject(fileOut, object);
    }

    public Object kryoDeSerialization(String inFile){
       return
    }
}
