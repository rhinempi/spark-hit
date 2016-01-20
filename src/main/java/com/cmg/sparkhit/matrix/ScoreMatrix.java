package com.cmg.sparkhit.matrix;

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
public class ScoreMatrix implements Serializable, ShMatrix {

    final int maxGapNum = 4096;
    final int maxNtTypes = 6;
    final int[] BLOSUM62 = {
            1,                // A [0]
            -2, 1,            // C [1]
            -2,-2, 1,         // G [2]
            -2,-2,-2, 1,      // T [3]
            -2,-2,-2, 1, 1,   // U [4]
            -2,-2,-2,-2,-2, 1 // N [5]
        //  A  C  G  T  U  N
    };

    public ScoreMatrix(){
        initiateMatrix(-6, -1); // not make them program parameters for the moment
    }

    public int[] gapArray = new int[maxGapNum];
    public int[][] matrix = new int[maxNtTypes][maxNtTypes];

    public void initiateMatrix(int gap, int extendGap){
        for(int i = 0; i<maxGapNum; i++){
            gapArray[i] = gap + i*extendGap;
        }

        int k=0;
        for (int i =0; i< maxNtTypes; i++){
            for(int j=0; j<= i; j++){
                matrix[i][j]=matrix[j][i] = BLOSUM62[k++];
            }
        }
    }
}
