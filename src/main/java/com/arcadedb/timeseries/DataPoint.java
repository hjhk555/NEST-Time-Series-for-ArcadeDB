package com.arcadedb.timeseries;

import com.arcadedb.database.Binary;

public abstract class DataPoint {
    public long timestamp;

    public static int bytesToWrite(DataType type) throws TimeseriesException {
        return switch (type.baseType){
            case LONG -> 16;
            case STRING -> 8 + Statistics.bytesToWriteUnsignedNumber(type.param) + type.param;
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public static DataPoint getDataPointFromBinary(DataType dataType, Binary binary) throws TimeseriesException {
        return switch (dataType.baseType){
            case LONG -> new LongDataPoint(binary.getLong(), binary.getLong());
            case STRING -> new StringDataPoint(binary.getLong(), binary.getString());
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public abstract void serialize(Binary binary);
}
