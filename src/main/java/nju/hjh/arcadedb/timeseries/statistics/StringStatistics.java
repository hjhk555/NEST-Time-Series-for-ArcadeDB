package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.MathUtils;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.List;
import java.util.NavigableMap;

public class StringStatistics extends FixedStatistics{
    public String firstValue;
    public String lastValue;

    public StringStatistics(){
        firstValue = "";
        lastValue = "";
    }

    @Override
    public void clear() {
        super.clear();
        firstValue = "";
        lastValue = "";
    }

    @Override
    public void insert(DataPoint data) throws TimeseriesException {
        if (data instanceof StringDataPoint sData){
            if (count == 0){
                count = 1;
                firstTime = sData.timestamp;
                firstValue = sData.value;
                lastTime = sData.timestamp;
                lastValue = sData.value;
            }else{
                count++;
                if (sData.timestamp < firstTime){
                    firstTime = sData.timestamp;
                    firstValue = sData.value;
                } else if (sData.timestamp > lastTime){
                    lastTime = sData.timestamp;
                    lastValue = sData.value;
                }
            }
        }else{
            throw new TimeseriesException("StringStatistics can only handle StringDataPoint");
        }
    }

    @Override
    public boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (oldDP instanceof StringDataPoint oldSDP && newDP instanceof StringDataPoint newSDP){
            if (oldDP.timestamp != newDP.timestamp)
                throw new TimeseriesException("timestamp different when updating statistics");
            if (oldSDP.timestamp == firstTime){
                firstValue = newSDP.value;
            }
            if (oldSDP.timestamp == lastTime){
                lastValue = newSDP.value;
            }
            return true;
        }else{
            throw new TimeseriesException("StringStatistics can only handle StringDataPoint");
        }
    }

    @Override
    public void insertAll(NavigableMap<Long, DataPoint> datapoints) {
        if (datapoints.size() == 0) return;

        count += datapoints.size();
        StringDataPoint listFirst = (StringDataPoint) datapoints.firstEntry().getValue();
        StringDataPoint listLast = (StringDataPoint) datapoints.lastEntry().getValue();
        if (listFirst.timestamp < firstTime) {
            firstTime = listFirst.timestamp;
            firstValue = listFirst.value;
        }
        if (listLast.timestamp > lastTime) {
            lastTime = listLast.timestamp;
            lastValue = listLast.value;
        }
    }

    @Override
    public void merge(Statistics stats) throws TimeseriesException {
        if (stats == null || stats.count == 0)
            return;
        if (stats instanceof StringStatistics sStats){
            count += sStats.count;
            if (sStats.firstTime < this.firstTime){
                firstTime = sStats.firstTime;
                firstValue = sStats.firstValue;
            }
            if (sStats.lastTime > this.lastTime){
                lastTime = sStats.lastTime;
                lastValue = sStats.lastValue;
            }
        }else{
            throw new TimeseriesException("StringStatistics can only merge StringStatistics");
        }
    }

    /**
     * return bytes needed to write StringStatistics
     * long(8B) * 3 + String(length) * 2
     * @param length max length of string
     */
    public static int maxBytesRequired(int length){
        int bytesToWriteString = MathUtils.bytesToWriteUnsignedNumber(length) + length;
        return 24 + 2 * bytesToWriteString;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(count);
        binary.putLong(firstTime);
        binary.putLong(lastTime);
        binary.putString(firstValue);
        binary.putString(lastValue);
    }

    @Override
    public void deserialize(Binary binary) {
        count = binary.getLong();
        firstTime = binary.getLong();
        lastTime = binary.getLong();
        firstValue = binary.getString();
        lastValue = binary.getString();
    }

    @Override
    public Statistics clone() {
        StringStatistics newStats = new StringStatistics();
        newStats.count = count;
        newStats.firstTime = firstTime;
        newStats.firstValue = firstValue;
        newStats.lastTime = lastTime;
        newStats.lastValue = lastValue;
        return newStats;
    }

    @Override
    public String toPrettyPrintString() {
        return String.format("StringStatistics{\n\tcount=%d\n\tfirstTime=%d\n\tfirstValue=%s\n\tlastTime=%d\n\tlastValue=%s\n}",
                count, firstTime, firstValue, lastTime, lastValue);
    }

    @Override
    public Object getFirstValue() {
        return firstValue;
    }

    @Override
    public Object getLastValue() {
        return lastValue;
    }
}
