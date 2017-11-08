package uni.bielefeld.cmg.sparkhit.io;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 02.11.17.
 *
 *      Sparkhit
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * FragRec is free software: you can redistribute it and/or modify it
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
 */

public class FastqUnitBufferTest  extends TestCase {
    protected BufferedReader inputBuffer;

    @Test (expected = IOException.class)
    public void testFastqUnitBufferException() {
        FastqUnitBuffer fqbuffer = new FastqUnitBuffer(inputBuffer);
        try {
            fqbuffer.loadBufferedFastq();
        }catch(RuntimeException e){

        }
    }

    protected void setUp(){
        TextFileBufferInput inputfile = new TextFileBufferInput();
        inputfile.setInput("src/test/resources/fastq.fq");
        inputBuffer = inputfile.getBufferReader();
    }
}