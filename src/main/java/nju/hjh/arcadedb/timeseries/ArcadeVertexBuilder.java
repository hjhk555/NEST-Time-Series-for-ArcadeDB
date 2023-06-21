package nju.hjh.arcadedb.timeseries;

import com.arcadedb.graph.Vertex;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public interface ArcadeVertexBuilder {
    public ArcadeVertex build(Vertex vertex) throws TimeseriesException;
}
