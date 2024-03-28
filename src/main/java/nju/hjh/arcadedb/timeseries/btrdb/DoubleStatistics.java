package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.Binary;

public class DoubleStatistics {
    public double min;
    public double max;
    public double sum;
    public int cnt;
    public static final DoubleStatistics EMPTY_STATISTICS = new DoubleStatistics();

    public DoubleStatistics(){
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        sum = 0;
        cnt = 0;
    }

    public void append(DoubleStatistics statistics){
        min = Math.min(min, statistics.min);
        max = Math.max(max, statistics.max);
        sum += statistics.sum;
        cnt += statistics.cnt;
    }

    public void append(double value){
        min = Math.min(min, value);
        max = Math.max(max, value);
        sum += value;
        cnt ++;
    }

    public void serialize(Binary binary) {
        binary.putInt(cnt);
        binary.putLong(Double.doubleToLongBits(sum));
        binary.putLong(Double.doubleToLongBits(max));
        binary.putLong(Double.doubleToLongBits(min));
    }

    public void deserialize(Binary binary){
        cnt = binary.getInt();
        sum = Double.longBitsToDouble(binary.getLong());
        max = Double.longBitsToDouble(binary.getLong());
        min = Double.longBitsToDouble(binary.getLong());
    }

    public DoubleStatistics clone(){
        DoubleStatistics newStatistics = new DoubleStatistics();
        newStatistics.cnt = this.cnt;
        newStatistics.sum = this.sum;
        newStatistics.max = this.max;
        newStatistics.min = this.min;
        return newStatistics;
    }

    public String toPrettyPrintString() {
        return String.format("{\n\tcount=%d\n\tsum=%f\n\tmax=%f\n\tmin=%f}", cnt, sum, max, min);
    }
}
