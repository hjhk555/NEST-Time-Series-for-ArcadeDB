package nju.hjh.arcadedb.timeseries;

public class TSUpdateStrategy {
    public static TSUpdateStrategy IGNORE;
    public static TSUpdateStrategy ERROR;
    public static TSUpdateStrategy UPDATE;
    public static TSUpdateStrategy APPEND;

    static {
        IGNORE = new TSUpdateStrategy(TSBaseUpdateStrategy.IGNORE);
        ERROR = new TSUpdateStrategy(TSBaseUpdateStrategy.ERROR);
        UPDATE = new TSUpdateStrategy(TSBaseUpdateStrategy.UPDATE);
        APPEND = new TSUpdateStrategy(TSBaseUpdateStrategy.APPEND);
    }

    public enum TSBaseUpdateStrategy {
        IGNORE,
        ERROR,
        UPDATE,
        APPEND
    }

    public TSBaseUpdateStrategy baseStrategy;
    public String separator;

    public TSUpdateStrategy(TSBaseUpdateStrategy baseStrategy){
        this(baseStrategy, "+");
    }

    public TSUpdateStrategy(TSBaseUpdateStrategy baseStrategy, String separator) {
        this.baseStrategy = baseStrategy;
        this.separator = separator;
    }
}
