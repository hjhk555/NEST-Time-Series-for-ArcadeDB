package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.MathUtils;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.List;

public class UnfixedStatistics extends Statistics{
    public UnfixedStatistics() {
    }

    @Override
    public void insert(DataPoint data) throws TimeseriesException {
        if (count == 0){
            count = 1;
            firstTime = data.timestamp;
            lastTime = data.timestamp;
        }else{
            count++;
            if (data.timestamp < firstTime){
                firstTime = data.timestamp;
            } else if (data.timestamp > lastTime){
                lastTime = data.timestamp;
            }
        }
    }

    @Override
    public boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (oldDP.timestamp != newDP.timestamp)
            throw new TimeseriesException("timestamp different when updating statistics");
        return true;
    }

    @Override
    public void insertDataList(List<DataPoint> dataList, boolean isTimeOrdered) {
        if (dataList.size() == 0) return;

        count += dataList.size();
        if (isTimeOrdered) {
            DataPoint listFirst = dataList.get(0);
            DataPoint listLast = dataList.get(dataList.size()-1);
            if (listFirst.timestamp < firstTime) {
                firstTime = listFirst.timestamp;
            }
            if (listLast.timestamp > lastTime) {
                lastTime = listLast.timestamp;
            }
        }else{
            for (DataPoint dataPoint : dataList) {
                if (dataPoint.timestamp < firstTime){
                    firstTime = dataPoint.timestamp;
                }
                if (dataPoint.timestamp > lastTime){
                    lastTime = dataPoint.timestamp;
                }
            }
        }
    }

    /**
     * return bytes needed to write UnfixedStatistics
     * long(8B) * 3
     */
    public static int maxBytesRequired(){
        return 24;
    }

    @Override
    public void merge(Statistics stats) throws TimeseriesException {
        if (stats == null || stats.count == 0)
            return;
        count += stats.count;
        if (stats.firstTime < this.firstTime) {
            firstTime = stats.firstTime;
        }
        if (stats.lastTime > this.lastTime) {
            lastTime = stats.lastTime;
        }
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(count);
        binary.putLong(firstTime);
        binary.putLong(lastTime);
    }

    @Override
    public void deserialize(Binary binary) {
        count = binary.getLong();
        firstTime = binary.getLong();
        lastTime = binary.getLong();
    }

    @Override
    public Statistics clone() {
        UnfixedStatistics newStats = new UnfixedStatistics();
        newStats.count = count;
        newStats.firstTime = firstTime;
        newStats.lastTime = lastTime;
        return newStats;
    }

    @Override
    public String toPrettyPrintString() {
        return String.format("UnfixedStatistics{\n\tcount=%d\n\tfirstTime=%d\n\tlastTime=%d\n}",
                count, firstTime, lastTime);
    }
}
