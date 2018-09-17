/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lytrax.accessconverter;

import com.healthmarketscience.jackcess.Column;
import java.math.BigDecimal;

/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
 */
public class Globals {
    public static double floatValue(Object value, Column column) {
        Byte precission = column.getPrecision();
        if(precission == 0) precission = 2;
        Double number = Double.valueOf(value.toString());
        BigDecimal bigDecimal = new BigDecimal(number);
        BigDecimal roundedWithScale = bigDecimal.setScale(precission, BigDecimal.ROUND_HALF_UP);
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
