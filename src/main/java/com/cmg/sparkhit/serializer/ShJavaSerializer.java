package com.cmg.sparkhit.serializer;

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
public interface ShJavaSerializer extends ShSerializer{

    /**
     * serialize objects with java default serializer
     *
     * @param object
     * @param outFile
     */
    void javaSerialization (Object object, String outFile);

    /**
     * de serialize objects with java default serialier
     *
     * @param inFile
     * @return
     */
    Object javaDeSerialization (String inFile);

    /**
     * this abstract method is invalid for this sub interface. Override it with null
     *
     * @param object
     * @param outFile
     */
    void kryoSerialization(Object object, String outFile);

    /**
     * this abstract method is invalid for this sub interface. Override it with null
     *
     * @param inFile
     * @return
     */
    Object kryoDeSerialization (String inFile);

}
