package nju.hjh.utils.log;

import java.text.SimpleDateFormat;

public class TimestampContent implements LogTagContent {
    static SimpleDateFormat timeStampFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss:SSS");

    static TimestampContent instance = new TimestampContent();

    public static TimestampContent getInstance(){
        return instance;
    }

    @Override
    public String getContent() {
        return timeStampFormat.format(System.currentTimeMillis());
    }
}
