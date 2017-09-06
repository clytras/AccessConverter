/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accessconverter;

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
}
