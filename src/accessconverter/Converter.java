/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accessconverter;

/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
 */
public abstract class Converter {
    String logSource(String method) {
        return String.format("%s:%s", this.getClass().getSimpleName(), method);
    }
    
    String logSource() {
        return String.format("%s", this.getClass().getSimpleName());
    }
    
    void Log(String text) {
        AccessConverter.Log(text, logSource());
    }
    
    void Log(String text, String source) {
        AccessConverter.Log(text, logSource(source));
    }
    
    void Error(String error) {
        AccessConverter.Error(error, null, logSource());
    }
    
    void Error(String error, Exception exception) {
        AccessConverter.Error(error, exception, logSource());
    }
    
    void Error(String error, Exception exception, String source) {
        AccessConverter.Error(error, exception, logSource(source));
    }
}
