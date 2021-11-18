package com.starrocks.connector.flink.tools;

public class DataUtil {

    public static String ClearBracket(String context) {
        int head = context.indexOf('(');
        if (head == -1) {
            return context;
        } else {
            int next = head + 1;
            int count = 1;
            do {
                if (context.charAt(next) == '(')
                count++;
                else if (context.charAt(next) == ')')
                count--;
                next++;
                if (count == 0) {
                    String temp = context.substring(head, next);
                    context = context.replace(temp, ""); 
                    head = context.indexOf('('); 
                    next = head + 1; 
                    count = 1; 
                }
            } while (head != -1); 
        }
        return context; 
    }

    public static String addZeroForNum(String str, int strLength) {
        int strLen = str.length();
        if (strLen < strLength) {
            while (strLen < strLength) {
                StringBuffer sb = new StringBuffer();
                sb.append(str).append("0");
                str = sb.toString();
                strLen = str.length();
            }
        }
        return str;
    }
}
