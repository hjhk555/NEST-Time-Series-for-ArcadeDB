package nju.hjh.arcadedb.timeseries;

import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.ArrayList;
import java.util.List;

public class DataPointList {
    // content of list
    public ArrayList<DataPoint> dataPointList = new ArrayList<>();
    // index of current data point in list
    public int curIndex = -1;

    // get next datapoint
    public DataPoint next() throws TimeseriesException {
        if (curIndex >= dataPointList.size()-1) return null;
        return dataPointList.get(++curIndex);
    }

    // query if next data point exist
    public boolean hasNext() throws TimeseriesException {
        return curIndex < dataPointList.size()-1;
    }

    public List<DataPoint> getList(){
        return dataPointList;
    }
}
