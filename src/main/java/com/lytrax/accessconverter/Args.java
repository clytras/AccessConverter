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

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class Args {

    public Map<String, String> options;
    public Map<String, Boolean> flags;

    public Args(String[] args) {
        options = new HashMap<>();
        flags = new HashMap<>();
        int i = 0;

        while (i < args.length) {
            if (args[i].startsWith("--")) {
                String name = args[i].substring(2);

                if (++i < args.length) {
                    String value = args[i];
                    options.put(name, value);
                }
            } else if (args[i].startsWith("-")) {
                String name = args[i].substring(1);
                flags.put(name, true);
            }

            i++;
        }
    }

    public boolean HasOption(String name) {
        return options.containsKey(name);
    }

    public String GetOption(String name) {
        return GetOption(name, "");
    }

    public String GetOption(String name, String defaultValue) {
        if (options.containsKey(name)) {
            return options.get(name);
        }

        return defaultValue;
    }

    public boolean HasFlag(String name) {
        return flags.containsKey(name);
    }

    public boolean GetFlag(String name) {
        return GetFlag(name, false);
    }

    public boolean GetFlag(String name, boolean defaultValue) {
        if (flags.containsKey(name)) {
            return flags.get(name);
        }

        return defaultValue;
    }
}
