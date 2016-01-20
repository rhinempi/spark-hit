package com.cmg.sparkhit.struct;

import java.io.Serializable;

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
public class ReadInfo implements Serializable {
    public String line;
    public String readName;
    public String read;
    public int readSize;

    public ReadInfo(String line){
        /**
         * a data structure storing NGS reads info
         */
        this.line = line;
        logInfo();
    }

    public void logInfo(){
        String[] textFq = line.split("\t");
        name(textFq[0]);
        seq(textFq[1]);
        size(read);
    }

    public void name(String n){
        this.readName = n;
    }

    public void seq(String r){
        this.read = r;
    }

    public void size(String s){
        this.readSize = s.length();
    }
}
