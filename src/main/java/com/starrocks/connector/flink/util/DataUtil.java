package com.starrocks.connector.flink.util;

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
}
