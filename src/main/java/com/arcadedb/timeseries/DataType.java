package com.arcadedb.timeseries;

import com.arcadedb.database.Binary;

public class DataType {
    public enum BaseType{
        LONG((byte) 0, "LONG"),
        STRING((byte) 1, "STRING");

        final byte seq;
        final String name;

        BaseType(byte seq, String name) {
            this.seq = seq;
            this.name = name;
        }
    }

    public BaseType baseType;
    public int param;

    public void checkValid() throws TimeseriesException {
        switch (baseType){
            case STRING -> {
                if (param <= 0)
                    throw new TimeseriesException("length of string should be positive");
                if (param >= 1000)
                    throw new TimeseriesException("length of string should less than 1000");
            }
        }
    }

    public DataType(BaseType baseType, int param) throws TimeseriesException {
        this.baseType = baseType;
        this.param = param;
        checkValid();
    }

    public void serialize(Binary binary){
        binary.putByte(baseType.seq);
        binary.putInt(param);
    }

    public static DataType resolveFromBinary(Binary binary) throws TimeseriesException {
        byte bBaseType = binary.getByte();
        return switch (bBaseType) {
            case 0 -> new DataType(BaseType.LONG, binary.getInt());
            case 1 -> new DataType(BaseType.STRING, binary.getInt());
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public DataPoint checkAndConvertDataPoint(DataPoint dataPoint) throws TimeseriesException {
        switch (baseType){
            case LONG -> {
                if (dataPoint instanceof LongDataPoint){
                    return dataPoint;
                }else{
                    throw new TimeseriesException("unmatched data and type(Long)");
                }
            }
            case STRING -> {
                if (dataPoint instanceof StringDataPoint strDP){
                    if (strDP.value.length() > param)
                        throw new TimeseriesException(String.format("string lwngth(%d) exceeded limit(%d)", strDP.value.length(), param));
                    return dataPoint;
                }else{
                    throw new TimeseriesException("unmatched data and type(String)");
                }
            }
            default -> {
                throw new TimeseriesException("invalid data type");
            }
        }
    }
}
