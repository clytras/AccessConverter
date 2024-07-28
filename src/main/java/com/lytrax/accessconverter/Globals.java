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

import com.healthmarketscience.jackcess.Column;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class Globals {
    public static final String OUTPUT_SQLITE = "sqlite";
    public static final String OUTPUT_MYSQL = "mysql";
    public static final String OUTPUT_JSON = "json";

    public static double floatValue(Object value, Column column) {
        Byte precission = column.getPrecision();
        if(precission == 0) precission = 2;
        Double number = Double.valueOf(value.toString());
        BigDecimal bigDecimal = new BigDecimal(number);
        BigDecimal roundedWithScale = bigDecimal.setScale(precission, RoundingMode.HALF_UP);
        return Double.valueOf(roundedWithScale.toString());
    }
    
    public static long defaultIfNullLong(Long l) {
        return defaultIfNullLong(l, 0);
    }

    public static long defaultIfNullLong(Long l, long defaultValue) {
        if(l == null)
            return defaultValue;
        else
            return l;
    }
    
    public static double defaultIfNullInteger(Integer i) {
        return defaultIfNullInteger(i, 0);
    }

    public static double defaultIfNullInteger(Integer i, int defaultValue) {
        if(i == null)
            return defaultValue;
        else
            return i;
    }
    
    public static double defaultIfNullFloat(Float f) {
        return defaultIfNullFloat(f, 0.0f);
    }

    public static double defaultIfNullFloat(Float f, float defaultValue) {
        if(f == null)
            return defaultValue;
        else
            return f;
    }

    public static double defaultIfNullDouble(Double d) {
        return defaultIfNullDouble(d, 0.0);
    }

    public static double defaultIfNullDouble(Double d, double defaultValue) {
        if(d == null)
            return defaultValue;
        else
            return d;
    }
    
    public static double defaultIfNullBigDecimal(BigDecimal d) {
        return defaultIfNullBigDecimal(d, 0.0);
    }

    public static double defaultIfNullBigDecimal(BigDecimal d, double defaultValue) {
        if(d == null)
            return defaultValue;
        else
            return d.doubleValue();
    }
}
