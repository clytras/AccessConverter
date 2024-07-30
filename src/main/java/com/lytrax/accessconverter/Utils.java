/*
 * The MIT License
 *
 * Copyright 2024 Christos Lytras <christos.lytras@gmail.com>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.lytrax.accessconverter;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */

public class Utils {
    public static String removeQuotation(String s, char c) {
        if(s.charAt(0) == c && s.charAt(s.length() - 1) == c)
            return s.substring(1, s.length() - 1);
        else
            return s;
    }

    public static String removeQuotation(String s) {
        return removeQuotation(s, '"');
    }

    public static String quoteSqlName(String name) {
        return "`" + name + "`";
    }

    public static <T> List<String> quoteSqlNames(List<T> names) {
        return names.stream().map(s -> quoteSqlName(s.toString())).collect(Collectors.toList());
    }

    public static String booleanDefaultValue(String value, String trueValue, String falseValue) {
        if(value.equalsIgnoreCase("Yes")) {
            return trueValue;
        }

        if (value.equalsIgnoreCase("No")) {
            return falseValue;
        }

        return value;
    }

    public static String booleanDefaultValue(String value) {
        return booleanDefaultValue(value, "1", "0");
    }

    public static Boolean isDatetimeNow(String value) {
        return value.matches("(?i)=?Now.*");
    }

    public static String datetimeDefaultValue(String value, String now) {
        if (isDatetimeNow(value)) {
            return now;
        }

        return value;
    }

    public static <T> String valueOrNull(T value) {
        if(value == null) {
            return "NULL";
        }

        return value.toString();
    }

    public static String escapeSingleQuotes(String value) {
        return value.replace("'", "''");
    }
}
