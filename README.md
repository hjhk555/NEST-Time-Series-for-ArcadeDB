# NEST : NodE with Statistics Tree

NEST is a tree-like data structrue for the storage of time series. It managed to split the data points in time series into several nodes and store them inside the document of ArcadeDB. Moreover, NEST provide support by storing and updating statistics in each node, which can be used to accelerate the aggregation operations on the time series it stores.