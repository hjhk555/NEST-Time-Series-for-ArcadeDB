package nju.hjh.arcadedb.timeseries;

import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.text.ParseException;

public class UpdateStrategy {
    public static UpdateStrategy IGNORE;
    public static UpdateStrategy ERROR;
    public static UpdateStrategy UPDATE;
    public static UpdateStrategy APPEND;

    static {
        IGNORE = new UpdateStrategy(TSBaseUpdateStrategy.IGNORE);
        ERROR = new UpdateStrategy(TSBaseUpdateStrategy.ERROR);
        UPDATE = new UpdateStrategy(TSBaseUpdateStrategy.UPDATE);
        APPEND = new UpdateStrategy(TSBaseUpdateStrategy.APPEND);
    }

    public enum TSBaseUpdateStrategy {
        IGNORE,
        ERROR,
        UPDATE,
        APPEND
    }

    public TSBaseUpdateStrategy baseStrategy;
    public String separator;

    public UpdateStrategy(TSBaseUpdateStrategy baseStrategy){
        this(baseStrategy, "+");
    }

    public UpdateStrategy(TSBaseUpdateStrategy baseStrategy, String separator) {
        this.baseStrategy = baseStrategy;
        this.separator = separator;
    }

    public static UpdateStrategy parse(String str) throws TimeseriesException {
        if (str.equals("ignore"))
            return IGNORE;
        if (str.equals("error"))
            return ERROR;
        if (str.equals("update"))
            return UPDATE;
        if (str.equals("append"))
            return APPEND;
        if (str.startsWith("append(") && str.endsWith(")")){
            return new UpdateStrategy(TSBaseUpdateStrategy.APPEND, str.substring("append(".length(), str.length()-1).trim());
        }
        throw new TimeseriesException("unknown update strategy: "+str);
    }
}
