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

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
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
    
    void Error(String error, Exception exception, String source, String sql) {
        AccessConverter.Error(error, exception, logSource(source), sql);
    }
}
