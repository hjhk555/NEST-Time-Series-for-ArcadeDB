package nju.hjh.arcadedb.timeseries.client;

public class ClientUtils {
    public static final String DEFAULT_SERVER_HOST = "127.0.0.1";
    public static final int DEFAULT_SERVER_PORT = 8809;
    public static final int SHOW_MESSAGE_LENGTH = 1000;

    public static String toPrettyPrintJSON(String json){
        StringBuilder strBuilder = new StringBuilder();
        StringBuilder prefix = new StringBuilder();
        boolean insideString = false;
        boolean trope = false;
        for (char ch : json.toCharArray()){
            switch (ch) {
                case '{', '[' -> {
                    strBuilder.append(ch);
                    if (!insideString) {
                        prefix.append('\t');
                        strBuilder.append('\n').append(prefix);
                    }
                }
                case '}', ']' -> {
                    if (!insideString) {
                        prefix.deleteCharAt(prefix.length() - 1);
                        strBuilder.append('\n').append(prefix);
                    }
                    strBuilder.append(ch);
                }
                case '"' -> {
                    if (!trope)
                        insideString = !insideString;
                    strBuilder.append(ch);
                }
                case ',' -> {
                    strBuilder.append(ch);
                    if (!insideString){
                        strBuilder.append('\n').append(prefix);
                    }
                }
                case '\n','\t',' ' -> {
                    // ignore empty characters outside string
                    if (insideString) strBuilder.append(ch);
                }
                default -> strBuilder.append(ch);
            }
            if (trope)
                trope = false;
            else if (ch == '\\')
                trope = true;
        }
        return strBuilder.toString();
    }
}
