/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
