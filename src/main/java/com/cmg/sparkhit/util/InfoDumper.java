package com.cmg.sparkhit.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
public class InfoDumper implements InfoManager{

    private String message;

    public InfoDumper (){
        /**
         * format output information and message
         */
    }

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerMessage(String timeFormat) {
        String mightyName = "SparkHit ";
        String currentTime = headerTime(timeFormat);
        String mHeader = mightyName + currentTime;
        return mHeader;
    }

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerTime(String timeFormat){
        SimpleDateFormat hourMinuteSecond = new SimpleDateFormat(timeFormat);
        String timeHeader = hourMinuteSecond.format(new Date());
        return timeHeader;
    }

    /**
     *
     * @param m
     * @return
     */
    private String completeMessage(String m){
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + m;
        return completedMessage;
    }

    /**
     * out put formatted messages
     */
    public void screenDump(){
        System.out.println(message);
    }

    /**
     *
     * @param m
     */
    public void readMessage(String m) {
        this.message = completeMessage(m);
    }

    /**
     *
     * @param e
     */
    public void readIOException(IOException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    /**
     *
     * @param e
     */
    public void readFileNotFoundException(FileNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    public void readClassNotFoundException(ClassNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }
}
