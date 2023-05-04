package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Document;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public interface ArcadeDocumentBuilder {
    public ArcadeDocument build(Document document) throws TimeseriesException;
}
