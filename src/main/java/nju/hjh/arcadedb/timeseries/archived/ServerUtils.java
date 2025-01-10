package nju.hjh.arcadedb.timeseries.archived;

public class ServerUtils {
    public static final int DEFAULT_SERVER_PORT = 8809;
    public static final int SHOW_MESSAGE_LENGTH = 1000;
    public static final String DATABASE_DIR = "./databases/";
    public static final String CONNECTION_CLOSE = "close";
    public static final int MAX_LISTALL_SIZE = 1000;

    // message key
    public static class Key {
        public final static String IN_DATABASE = "db";
        public final static String IN_ACTION = "act";
        public final static String IN_INSERT = "ins";
        public final static String IN_QUERY = "qry";
        public final static String IN_STRATEGY = "stg";
        public final static String MANAGE_TYPE = "typ";
        public final static String STRATEGY_TYPE = IN_STRATEGY;
        public final static String STRATEGY_SEPARATOR = "sep";
        public final static String INSERT_QUERY_OBJECT = "obj";
        public final static String INSERT_TAG = "tag";
        public final static String INSERT_TAG_OVERWRITE = "tow";
        public final static String INSERT_TIMESERIES_FORMAT = "fmt";
        public final static String INSERT_TIMESERIES = "ts";
        public final static String TIMESERIES_TIMESTAMP = "t";
        public final static String TIMESERIES_VALUE = "v";
        public final static String QUERY_TYPE = MANAGE_TYPE;
        public final static String QUERY_MULTIPLE = "mlt";
        public final static String QUERY_METRIC = "mtc";
        public final static String QUERY_BEGIN_TIMESTAMP = "bgn";
        public final static String QUERY_END_TIMESTAMP = "end";
        public final static String QUERY_LIMIT = "lmt";
        public final static String OUT_ERROR = "err";
        public final static String OUT_SUCCESS = "suc";
        public final static String OUT_RESULT = "res";
        public final static String OUT_TIMESTAMP = TIMESERIES_TIMESTAMP;
        public final static String OUT_VALUE = TIMESERIES_VALUE;
        public final static String OUT_TAG = INSERT_TAG;
        public final static String OUT_METRIC = QUERY_METRIC;
        public final static String ERROR_CLASS = "c";
        public final static String ERROR_MESSAGE = "m";
    }

    // message value
    public static class Value{
        public final static String DEFAULT_DATABASE = "tsdb";
        public final static String STRATEGY_TYPE_IGNORE = "ignore";
        public final static String STRATEGY_TYPE_ERROR = "error";
        public final static String STRATEGY_TYPE_UPDATE = "update";
        public final static String STRATEGY_TYPE_APPEND = "append";
        public final static String DEFAULT_STRATEGY_TYPE = STRATEGY_TYPE_IGNORE;
        public final static String DEFAULT_SEPARATOR = "+";
        public final static String ACTION_TYPE_MANAGE = "manage";
        public final static String ACTION_TYPE_INSERT = "insert";
        public final static String ACTION_TYPE_QUERY = "query";
        public final static String MANAGE_TYPE_CREATE = "create";
        public final static String MANAGE_TYPE_DROP = "drop";
        public final static String MANAGE_TYPE_EXIST = "exist";
        public final static String TIMESERIES_FORMAT_DATAPOINT = "point";
        public final static String TIMESERIES_FORMAT_TIMEVALUE_ARRAY = "array";
        public final static String TIMESERIES_FORMAT_TIMEVALUE_TABLE = "table";
        public final static String TIMESERIES_FORMAT_NULL = "null";
        public final static boolean DEFAULT_QUERY_MULTIPLE = false;
        public final static String QUERY_TYPE_INFO = "info";
        public final static String METRIC_QUERY_TYPE_LISTALL = "list";
        public final static String METRIC_QUERY_TYPE_FIRST = "first";
        public final static String METRIC_QUERY_TYPE_LAST = "last";
        public final static String METRIC_QUERY_TYPE_COUNT = "count";
        public final static String METRIC_QUERY_TYPE_MAX = "max";
        public final static String METRIC_QUERY_TYPE_MIN = "min";
        public final static String METRIC_QUERY_TYPE_SUM = "sum";
        public final static String METRIC_QUERY_TYPE_AVERAGE = "avg";
        public final static String QUERY_OBJECT_PREFIX_RID = "rid{";
        public final static String QUERY_OBJECT_PREFIX_OBJECT_ID = "oid{";
        public final static String QUERY_OBJECT_PREFIX_SQL = "sql{";
    }
}
