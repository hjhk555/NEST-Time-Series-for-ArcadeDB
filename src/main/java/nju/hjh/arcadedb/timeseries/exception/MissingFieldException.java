package nju.hjh.arcadedb.timeseries.exception;

public class MissingFieldException extends MessageParsingException {
    public MissingFieldException(String fieldName) {
        super("field '"+ fieldName +"' not found");
    }
}
