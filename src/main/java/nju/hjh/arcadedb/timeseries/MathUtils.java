package nju.hjh.arcadedb.timeseries;

public class MathUtils {
    public static int bytesToWriteUnsignedNumber(long number) {
        int bytes = 1;
        number >>>= 7;
        while (number != 0) {
            bytes++;
            number >>>= 7;
        }
        return bytes;
    }
}
