/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accessconverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
 */
public class Args {

    public Map<String, String> options;
    public Map<String, Boolean> flags;
    
    public Args(String[] args) {
        
        options = new HashMap<>();
        flags = new HashMap<>();
        
        int i = 0;
        
        while(i < args.length) {
            if(args[i].startsWith("--")) {
                String name = args[i].substring(2);
                if(++i < args.length) {
                    String value = args[i];
                    options.put(name, value);
                }
            } else
            if(args[i].startsWith("-")) {
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
        if(options.containsKey(name))
            return options.get(name);
        return defaultValue;
    }
    
    public boolean HasFlag(String name) {
        return flags.containsKey(name);
    }
    
    public boolean GetFlag(String name) {
        return GetFlag(name, false);
    }
    
    public boolean GetFlag(String name, boolean defaultValue) {
        if(flags.containsKey(name))
            return flags.get(name);
        return defaultValue;
    }
}

