package com.arcadedb.timeseries;

import com.arcadedb.database.Binary;

public class StringDataPoint extends DataPoint{
    public String value;

    public StringDataPoint(long timestamp, String value){
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(timestamp);
        binary.putString(value);
    }
}
