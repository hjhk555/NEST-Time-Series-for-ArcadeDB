package com.arcadedb.timeseries;

import com.arcadedb.database.RID;

import java.util.ArrayList;

public class DataPointSet {
    // preferred page size
    public static final int PREFERRED_DATALIST_SIZE = 10000;
    // query's start time
    public final long queryStartTime;
    // query's start time
    public final long queryEndTime;
    // rid of first leaf block
    public final RID firstLeafRID;
    public final ArcadeDocumentManager manager;
    public final String measurement;
    public final int degree;
    public final DataType dataType;
    public ArrayList<DataPoint> dataPointList = new ArrayList<>();
    // index of current data point in list
    public int curIndex;
    // start time for next page load
    public long nextStartTime;
    // start leaf's rid for next page load
    public RID nextBlockRID;

    public DataPointSet(long queryStartTime, long queryEndTime, RID firstLeafRID, ArcadeDocumentManager manager, String measurement, int degree, DataType dataType) {
        this.queryStartTime = queryStartTime;
        this.queryEndTime = queryEndTime;
        this.firstLeafRID = firstLeafRID;
        this.manager = manager;
        this.measurement = measurement;
        this.degree = degree;
        this.dataType = dataType;
        curIndex = -1;
        nextStartTime = queryStartTime;
        nextBlockRID = firstLeafRID;
    }

    /**
     * load next page
     * @return false if no next page
     */
    public boolean loadNextPage() throws TimeseriesException {
        dataPointList.clear();
        curIndex = -1;
        if (nextStartTime > queryEndTime || !nextBlockRID.isValid())
            return false;

        while (nextBlockRID.isValid() && dataPointList.size() < PREFERRED_DATALIST_SIZE) {
            StatsBlockLeaf currentLeaf = (StatsBlockLeaf) StatsBlock.getStatsBlockNonRoot(manager, nextBlockRID, null, measurement, degree, dataType, -1, false);
            currentLeaf.loadData();

            int currentSize = currentLeaf.dataList.size();
            int startPos, endPos;

            startPos = -1;
            if (currentLeaf.statistics.firstTime >= nextStartTime)
                // start from head
                startPos = 0;
            else{
                // binary search
                int low = 0, high = currentSize - 1;
                while (low <= high) {
                    int mid = (low + high) >>> 1;
                    long midTime = currentLeaf.dataList.get(mid).timestamp;
                    if (midTime > nextStartTime)
                        high = mid - 1;
                    else if (midTime < nextStartTime)
                        low = mid + 1;
                    else {
                        startPos = mid;
                        break;
                    }
                }
                if (low > high)
                    startPos = low;
            }

            endPos = -2;
            if (currentLeaf.statistics.lastTime <= queryEndTime)
                // end at tail
                endPos = currentSize-1;
            else{
                // binary search
                int low = 0, high = currentSize - 1;
                while (low <= high) {
                    int mid = (low + high) >>> 1;
                    long midTime = currentLeaf.dataList.get(mid).timestamp;
                    if (midTime > queryEndTime)
                        high = mid - 1;
                    else if (midTime < queryEndTime)
                        low = mid + 1;
                    else {
                        endPos = mid;
                        break;
                    }
                }
                if (low > high)
                    endPos = high;
            }

            if (startPos <= endPos)
                dataPointList.addAll(currentLeaf.dataList.subList(startPos, endPos+1));

            nextBlockRID = currentLeaf.succRID;
            nextStartTime = currentLeaf.statistics.lastTime+1;
        }
        return !dataPointList.isEmpty();
    }

    // get next datapoint
    public DataPoint next() throws TimeseriesException {
        while (curIndex >= dataPointList.size()-1){
            // load next page
            if (!loadNextPage())
                return null;
        }

        return dataPointList.get(++curIndex);
    }
}
