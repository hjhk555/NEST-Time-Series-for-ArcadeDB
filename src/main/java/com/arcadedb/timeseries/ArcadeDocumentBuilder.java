package com.arcadedb.timeseries;

import com.arcadedb.database.Document;

public interface ArcadeDocumentBuilder {
    public ArcadeDocument build(Document document) throws TimeseriesException;
}
