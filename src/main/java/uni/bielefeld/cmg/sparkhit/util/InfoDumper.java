package uni.bielefeld.cmg.sparkhit.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
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

/**
 * Returns an object for printing messages in a structured format to the screen.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class InfoDumper implements InfoManager, Serializable{

    private String message;
    private String mightyName;

    public InfoDumper (){
        /**
         * format output information and message
         */
        this.mightyName = "SparkHit"; /* the mighty name is your program name which will be used as the header */
    }

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerMessage(String timeFormat) {
        String currentTime = headerTime(timeFormat);
        String mHeader = mightyName + " " + currentTime;
        return mHeader;
    }

    /**
     * This method formats the message by including the time information.
     *
     * @param timeFormat the raw system time format.
     * @return the formatted time
     */
    private String headerTime(String timeFormat){
        SimpleDateFormat hourMinuteSecond = new SimpleDateFormat(timeFormat);
        String timeHeader = hourMinuteSecond.format(new Date());
        return timeHeader;
    }

    private String paragraphReader(String m){
        String completedParagraph = "";
        String[] paragraphLines = m.split("\n");
        String mHeader = headerMessage("HH:mm:ss");
        for (String line : paragraphLines){
            completedParagraph += mHeader + " " + line + "\n";
        }
        completedParagraph = completedParagraph.trim();
        return completedParagraph;
    }

    /**
     *
     * @param m
     * @return
     */
    private String completeMessage(String m){
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + " " + m;
        return completedMessage;
    }

    /**
     * This method prints the structured message to the screen.
     */
    public void screenDump(){
        System.out.println(message);
    }

    /**
     * This method records a specified message.
     *
     * @param m the message that is going to be processed.
     */
    public void readMessage(String m) {
        this.message = completeMessage(m);
    }

    /**
     * This method records a paragraph of message.
     *
     * @param m the message that is going to be processed.
     */
    public void readParagraphedMessages(String m ){
        this.message = paragraphReader(m);
    }

    /**
     * This method records an IOException message.
     *
     * @param e the IOException message that is going to be processed.
     */
    public void readIOException(IOException e){
        String m = e.getMessage();
        m = "IOException " + m;
        this.message = completeMessage(m);
    }

    /**
     * This method records a FileNotFoundException message.
     *
     * @param e the FileNotFoundException message that is going to be processed.
     */
    public void readFileNotFoundException(FileNotFoundException e){
        String m = e.getMessage();
        m = "FileNotFoundException " + m;
        this.message = completeMessage(m);
    }

    /**
     * This method records a ClassNotFoundException message.
     *
     * @param e the ClassNotFoundException message that is going to be processed.
     */
    public void readClassNotFoundException(ClassNotFoundException e){
        String m = e.getMessage();
        m = "ClassNotFoundException " + m;
        this.message = completeMessage(m);
    }
}
