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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class SqlFileWriter implements AutoCloseable {
    private FileWriter writer;

    public SqlFileWriter(File file) throws IOException {
        writer = new FileWriter(file);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public void write(String str) throws IOException {
        writer.write(str);
    }

    public void write(Integer i) throws IOException {
        writer.write(i.toString());
    }

    public void write(TextStringBuilder sb) throws IOException {
        writer.write(sb.toString());
    }

    public void write(String format, Object... args) throws IOException {
        writer.write(String.format(format, args));
    }

    public void writeln(String str) throws IOException {
        writer.write(str);
        writer.write("\n");
    }

    public void writeln(String format, Object... args) throws IOException {
        writer.write(String.format(format, args));
        writer.write("\n");
    }

    public void writeNewLine() throws IOException {
        writer.write("\n");
    }

    public void flush() throws IOException {
        writer.flush();
    }
}
