package com.cmg.sparkhit.core;

import java.io.IOException;


/**
 * Created by Liren Huang on 13/01/16.
 *
 *      FragRec
 *
 * Copyright (c) 2015-2015:
 *      Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */

import com.cmg.sparkhit.util.Parameter;
import com.cmg.sparkhit.util.DefaultParam;
import com.cmg.sparkhit.pipeline.Pipeline;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException{
        /* load parameters */
        Parameter parameter = new Parameter(args);  // get command line parameter
        DefaultParam param = parameter.importCommandLine(); // feed in default parameter

        /* start fragment recruitment pipeline */
        Pipeline pipeline = new Pipeline(param);
        pipeline.spark();   // launch spark
    }
}
