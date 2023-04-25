package com.arcadedb.timeseries;

import com.arcadedb.graph.Vertex;

public interface ArcadeVertexBuilder {
    public ArcadeVertex build(Vertex vertex) throws TimeseriesException;
}
