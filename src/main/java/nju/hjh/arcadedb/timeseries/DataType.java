package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public class DataType {
    public static final DataType LONG;
    public static final DataType STRING;
    public static final DataType DOUBLE;

    static {
        try {
            LONG = new DataType(BaseType.LONG, 0);
            STRING = new DataType(BaseType.STRING, 0);
            DOUBLE = new DataType(BaseType.DOUBLE, 0);
        } catch (TimeseriesException e) {
            // this should not happen
            throw new RuntimeException(e);
        }
    }

    public enum BaseType{
        LONG((byte) 0, "LONG"),
        STRING((byte) 1, "STRING"),
        DOUBLE((byte) 2, "DOUBLE");

        final byte seq;
        final String name;

        BaseType(byte seq, String name) {
            this.seq = seq;
            this.name = name;
        }
    }

    // the base-type of DataType
    public BaseType baseType;
    // param required by some of the base types
    public int param;

    public DataType(BaseType baseType, int param) throws TimeseriesException {
        this.baseType = baseType;
        this.param = param;
        checkValid();
    }

    public void serialize(Binary binary){
        binary.putByte(baseType.seq);
        binary.putInt(param);
    }

    /**
     * check if this data type is valid
     * @throws TimeseriesException if not valid
     */
    public void checkValid() throws TimeseriesException {
        switch (baseType){
            case STRING -> {
                if (param < 0)
                    throw new TimeseriesException("length of string should be 0 or positive");
                if (param > 2000)
                    throw new TimeseriesException("length of string should not exceed 2000");
            }
        }
    }

    /**
     * @return true if this data type has fixed size
     */
    public boolean isFixed(){
        if (baseType == BaseType.STRING && param == 0)
            // unlimited string
            return false;
        return true;
    }

    public static DataType resolveFromBinary(Binary binary) throws TimeseriesException {
        byte bBaseType = binary.getByte();
        return switch (bBaseType) {
            case 0 -> new DataType(BaseType.LONG, binary.getInt());
            case 1 -> new DataType(BaseType.STRING, binary.getInt());
            case 2 -> new DataType(BaseType.DOUBLE, binary.getInt());
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    /**
     * check and try to convert given data point into one with this data type
     * @param dataPoint given data point
     * @return converted data point
     * @throws TimeseriesException if failed to convert
     */
    public DataPoint checkAndConvertDataPoint(DataPoint dataPoint) throws TimeseriesException {
        switch (baseType){
            case LONG -> {
                if (dataPoint instanceof LongDataPoint){
                    return dataPoint;
                }
                if (dataPoint instanceof DoubleDataPoint doubleDP) {
                    return new LongDataPoint(dataPoint.timestamp, (long) doubleDP.value);
                }
                throw new TimeseriesException("unmatched data and type(Long)");
            }
            case DOUBLE -> {
                if (dataPoint instanceof DoubleDataPoint){
                    return dataPoint;
                }
                if (dataPoint instanceof LongDataPoint longDP){
                    return new DoubleDataPoint(dataPoint.timestamp, longDP.value);
                }
                throw new TimeseriesException("unmatched data and type(Double)");
            }
            case STRING -> {
                StringDataPoint strDP;
                if (dataPoint instanceof StringDataPoint) {
                    strDP = (StringDataPoint) dataPoint;
                } else {
                    strDP = new StringDataPoint(dataPoint.timestamp, dataPoint.getValue().toString());
                }

                int strLen = strDP.value.length();
                if (strLen > StringDataPoint.MAX_LENGTH)
                    throw new TimeseriesException(String.format("string length(%d) exceeded hard limit(%d)", strLen, StringDataPoint.MAX_LENGTH));
                if (param > 0 && strLen > param)
                    throw new TimeseriesException(String.format("string lwngth(%d) exceeded limit(%d)", strLen, param));
                return dataPoint;
            }
            default -> {
                throw new TimeseriesException("invalid data type");
            }
        }
    }
}
