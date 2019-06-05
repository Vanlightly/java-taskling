package com.siiconcatel.taskling.core.utils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class StringUtils {
    public static boolean isNullOrEmpty(String str) {
        if ( str == null) return true;
         return str.length() == 0;
    }

    public static boolean isNullOrWhitespace(String str) {
        if ( isNullOrEmpty(str)) return true;

        int len = str.length();
        for(int i=0; i<len; i++) {
            char ch = str.charAt(i);
            if(ch == ' ' || ch == '\n' || ch == '\t')
                return false;
        }

        return true;
    }

    public static String exceptionToString(Exception e)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        ps.close();
        return baos.toString();
    }
}
