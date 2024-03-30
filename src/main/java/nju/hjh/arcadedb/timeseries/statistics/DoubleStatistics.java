package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.List;
import java.util.NavigableMap;

public class DoubleStatistics extends NumericStatistics{
    public double firstValue;
    public double lastValue;
    public double sum;
    public double max;
    public double min;

    public DoubleStatistics() {
        sum = 0.0;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
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

    @Override
    public void insert(DataPoint data) throws TimeseriesException {
        if (data instanceof DoubleDataPoint dData) {
            if (count == 0) {
                count = 1;
                firstTime = dData.timestamp;
                lastTime = dData.timestamp;
                firstValue = dData.value;
                lastValue = dData.value;
                sum = dData.value;
                max = dData.value;
                min = dData.value;
            } else {
                count++;
                sum += dData.value;
                if (dData.timestamp < firstTime) {
                    firstTime = dData.timestamp;
                    firstValue = dData.value;
                } else if (dData.timestamp > lastTime) {
                    lastTime = dData.timestamp;
                    lastValue = dData.value;
                }
                if (dData.value > max)
                    max = dData.value;
                else if (dData.value < min)
                    min = dData.value;
            }
        }else{
            throw new TimeseriesException("DoubleStatistic can only handle DoubleDataPoint");
        }
    }

    @Override
    public boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (oldDP instanceof DoubleDataPoint oldDDP && newDP instanceof DoubleDataPoint newDDP){
            if (oldDP.timestamp != newDP.timestamp)
                throw new TimeseriesException("timestamp different when updating statistics");
            if (oldDDP.value == max){
                if (newDDP.value >= max){
                    max = newDDP.value;
                }else{
                    return false;
                }
            }
            if (oldDDP.value == min){
                if (newDDP.value <= max){
                    min = newDDP.value;
                }else{
                    return false;
                }
            }
            sum += newDDP.value - oldDDP.value;
            if (oldDDP.timestamp == firstTime){
                firstValue = newDDP.value;
            }
            if (oldDDP.timestamp == lastTime){
                lastValue = newDDP.value;
            }
            return true;
        }else{
            throw new TimeseriesException("DoubleStatistic can only handle DoubleDataPoint");
        }
    }

    @Override
    public void clear() {
        super.clear();
        sum = 0.0;
        this.max = Double.MIN_VALUE;
        this.min = Double.MAX_VALUE;
    }

    @Override
    public void insertAll(NavigableMap<Long, DataPoint> datapoints) {
        if (datapoints.size() == 0) return;

        count += datapoints.size();
        DoubleDataPoint listFirst = (DoubleDataPoint) datapoints.firstEntry().getValue();
        DoubleDataPoint listLast = (DoubleDataPoint) datapoints.lastEntry().getValue();
        if (listFirst.timestamp < firstTime) {
            firstTime = listFirst.timestamp;
            firstValue = listFirst.value;
        }
        if (listLast.timestamp > lastTime) {
            lastTime = listLast.timestamp;
            lastValue = listLast.value;
        }
        for (DataPoint dataPoint : datapoints.values()) {
            double value = ((DoubleDataPoint) dataPoint).value;
            sum += value;
            if (value > max)
                max = value;
            if (value < min)
                min = value;
        }
    }

    @Override
    public void merge(Statistics stats) throws TimeseriesException {
        if (stats == null || stats.count == 0)
            return;
        if (stats instanceof DoubleStatistics dStats){
            count += dStats.count;
            sum += dStats.sum;
            if (dStats.firstTime < this.firstTime){
                firstTime = dStats.firstTime;
                firstValue = dStats.firstValue;
            }
            if (dStats.lastTime > this.lastTime){
                lastTime = dStats.lastTime;
                lastValue = dStats.lastValue;
            }
            if (dStats.max > this.max)
                max = dStats.max;
            if (dStats.min < this.min)
                min = dStats.min;
        }else{
            throw new TimeseriesException("DoubleStatistics can only merge DoubleStatistics");
        }
    }

    /**
     * bytes needed to write LongStatistics
     * long(8B) * 3 + double(8B) * 5
     */
    public static int maxBytesRequired(){
        return 64;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(count);
        binary.putLong(firstTime);
        binary.putLong(lastTime);
        binary.putLong(Double.doubleToLongBits(firstValue));
        binary.putLong(Double.doubleToLongBits(lastValue));
        binary.putLong(Double.doubleToLongBits(sum));
        binary.putLong(Double.doubleToLongBits(max));
        binary.putLong(Double.doubleToLongBits(min));
    }

    @Override
    public void deserialize(Binary binary) {
        count = binary.getLong();
        firstTime = binary.getLong();
        lastTime = binary.getLong();
        firstValue = Double.longBitsToDouble(binary.getLong());
        lastValue = Double.longBitsToDouble(binary.getLong());
        sum = Double.longBitsToDouble(binary.getLong());
        max = Double.longBitsToDouble(binary.getLong());
        min = Double.longBitsToDouble(binary.getLong());
    }

    @Override
    public Statistics clone() {
        DoubleStatistics newStats = new DoubleStatistics();
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
        return String.format("DoubleStatistics{\n\tcount=%d\n\tfirstTime=%d\n\tfirstValue=%f\n\tlastTime=%d\n\tlastValue=%f\n\tsum=%f\n\tmax=%f\n\tmin=%f\n}",
                count, firstTime, firstValue, lastTime, lastValue, sum, max, min);
    }
}
