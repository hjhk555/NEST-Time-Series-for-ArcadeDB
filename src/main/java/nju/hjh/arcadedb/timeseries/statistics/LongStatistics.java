package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.List;

public class LongStatistics extends NumericStatistics{
    public long firstValue;
    public long lastValue;
    public long sum;
    public long max;
    public long min;

    public LongStatistics(){
        sum = 0;
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
    }

    @Override
    public void insert(DataPoint data) throws TimeseriesException {
        if (data instanceof LongDataPoint lData) {
            if (count == 0) {
                count = 1;
                firstTime = lData.timestamp;
                lastTime = lData.timestamp;
                firstValue = lData.value;
                lastValue = lData.value;
                sum = lData.value;
                max = lData.value;
                min = lData.value;
            } else {
                count++;
                sum += lData.value;
                if (lData.timestamp < firstTime) {
                    firstTime = lData.timestamp;
                    firstValue = lData.value;
                } else if (lData.timestamp > lastTime) {
                    lastTime = lData.timestamp;
                    lastValue = lData.value;
                }
                if (lData.value > max)
                    max = lData.value;
                else if (lData.value < min)
                    min = lData.value;
            }
        }else{
            throw new TimeseriesException("LongStatistic can only handle LongDataPoint");
        }
    }

    @Override
    public boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (oldDP instanceof LongDataPoint oldLDP && newDP instanceof LongDataPoint newLDP){
            if (oldDP.timestamp != newDP.timestamp)
                throw new TimeseriesException("timestamp different when updating statistics");
            if (oldLDP.value == max){
                if (newLDP.value >= max){
                    max = newLDP.value;
                }else{
                    return false;
                }
            }
            if (oldLDP.value == min){
                if (newLDP.value <= max){
                    min = newLDP.value;
                }else{
                    return false;
                }
            }
            sum += newLDP.value - oldLDP.value;
            if (oldLDP.timestamp == firstTime){
                firstValue = newLDP.value;
            }
            if (oldLDP.timestamp == lastTime){
                lastValue = newLDP.value;
            }
            return true;
        }else{
            throw new TimeseriesException("LongStatistic can only handle LongDataPoint");
        }
    }

    @Override
    public void insertDataList(List<DataPoint> dataList, boolean isTimeOrdered) {
        if (dataList.size() == 0) return;

        count += dataList.size();
        if (isTimeOrdered) {
            LongDataPoint listFirst = (LongDataPoint) dataList.get(0);
            LongDataPoint listLast = (LongDataPoint) dataList.get(dataList.size()-1);
            if (listFirst.timestamp < firstTime) {
                firstTime = listFirst.timestamp;
                firstValue = listFirst.value;
            }
            if (listLast.timestamp > lastTime) {
                lastTime = listLast.timestamp;
                lastValue = listLast.value;
            }
            for (DataPoint dataPoint : dataList) {
                long value = ((LongDataPoint) dataPoint).value;
                sum += value;
                if (value > max)
                    max = value;
                if (value < min)
                    min = value;
            }
        }else{
            for (DataPoint dataPoint : dataList) {
                long value = ((LongDataPoint) dataPoint).value;
                sum += value;
                if (value > max)
                    max = value;
                else if (value < min)
                    min = value;
                if (dataPoint.timestamp < firstTime){
                    firstTime = dataPoint.timestamp;
                    firstValue = value;
                }
                if (dataPoint.timestamp > lastTime){
                    lastTime = dataPoint.timestamp;
                    lastValue = value;
                }
            }
        }
    }

    @Override
    public void merge(Statistics stats) throws TimeseriesException {
        if (stats == null || stats.count == 0)
            return;
        if (stats instanceof LongStatistics lStats){
            count += lStats.count;
            sum += lStats.sum;
            if (lStats.firstTime < this.firstTime){
                firstTime = lStats.firstTime;
                firstValue = lStats.firstValue;
            }
            if (lStats.lastTime > this.lastTime){
                lastTime = lStats.lastTime;
                lastValue = lStats.lastValue;
            }
            if (lStats.max > this.max)
                max = lStats.max;
            if (lStats.min < this.min)
                min = lStats.min;
        }else{
            throw new TimeseriesException("LongStatistics can only merge LongStatistics");
        }
    }

    /**
     * bytes needed to write LongStatistics
     * long(8B) * 8
     */
    public static int maxBytesRequired(){
        return 64;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(count);
        binary.putLong(firstTime);
        binary.putLong(lastTime);
        binary.putLong(firstValue);
        binary.putLong(lastValue);
        binary.putLong(sum);
        binary.putLong(max);
        binary.putLong(min);
    }

    @Override
    public void deserialize(Binary binary) {
        count = binary.getLong();
        firstTime = binary.getLong();
        lastTime = binary.getLong();
        firstValue = binary.getLong();
        lastValue = binary.getLong();
        sum = binary.getLong();
        max = binary.getLong();
        min = binary.getLong();
    }

    @Override
    public Statistics clone() {
        LongStatistics newStats = new LongStatistics();
        newStats.count = count;
        newStats.firstTime = firstTime;
        newStats.firstValue = firstValue;
        newStats.lastTime = lastTime;
        newStats.lastValue = lastValue;
        newStats.sum = sum;
        newStats.max = max;
        newStats.min = min;
        return newStats;
    }

    @Override
    public String toPrettyPrintString() {
        return String.format("LongStatistics{\n\tcount=%d\n\tfirstTime=%d\n\tfirstValue=%d\n\tlastTime=%d\n\tlastValue=%d\n\tsum=%d\n\tmax=%d\n\tmin=%d\n}",
                count, firstTime, firstValue, lastTime, lastValue, sum, max, min);
    }

    @Override
    public Object getFirstValue() {
        return firstValue;
    }

    @Override
    public Object getLastValue() {
        return lastValue;
    }

    @Override
    public Number getSum() {
        return sum;
    }

    @Override
    public Number getMaxValue() {
        return max;
    }

    @Override
    public Number getMinValue() {
        return min;
    }
}
