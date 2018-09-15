/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accessconverter.accessconverter;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import java.io.IOException;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author zkr32
 */
public class ProgressStatus {
    public String currentTable;
    public int currentTableTotalRows;
    public int currentTableCurrentRow;
    
    public int totalRows;
    public int currentRow;
    public Database db;
    
    private long updateCheck;
    private int updateIntervalMillis;
    private short dotMarkCount = 0;
    private short maxDotMarks;
    private int resetCharsLength;
    
    public boolean enabled = false;
    
    private String status;
    
    public ProgressStatus(Database db) {
        this();
        this.db = db;
    }
    
    public ProgressStatus() {
        this.updateIntervalMillis = 1000;
        this.currentTableTotalRows = 0;
        this.currentTableCurrentRow = 0;
        this.maxDotMarks = 3;
        this.updateCheck = -1;
        this.status = "Initializing conversion";
        this.dotMarkCount = 0;
        this.totalRows = 0;
        this.currentRow = 0;
        this.enabled = AccessConverter.args.GetFlag("show-progress");
    }
    
    public void resetLine() {
        if(!this.enabled) return;
        System.out.printf("\r%s", StringUtils.repeat(" ", status.length() + dotMarkCount));
        System.out.print('\r');
        System.out.flush();
    }
    
    public void startTable(Table tbl) {
        if(!this.enabled) return;
        this.currentTable = tbl.getName();
        this.currentTableTotalRows = tbl.getRowCount();
        this.currentTableCurrentRow = 0;
        progress(true);
    }
    
    public void endTable() {
        if(!this.enabled) return;
        if(currentTableCurrentRow != currentTableTotalRows) {
            currentRow += currentTableTotalRows - currentTableCurrentRow;
        }
        progress(true);
    }
    
    public void step() {
        step(1);
    }
    
    public void step(int steps) {
        if(!this.enabled) return;
        currentTableCurrentRow += steps;
        currentRow += steps;
        progress(true);
    }
    
    private void progress() {
        progress(false);
    }
    
    private void progress(boolean doUpdate) {
        float percent = (float) (currentRow > 0 && totalRows > 0 ? ((double)currentRow / (double)totalRows) * 100.0 : 0);
        
        status = String.format("Total progress: %.1f%%; Table `%s` %d/%d", 
                percent, 
                currentTable, 
                currentTableCurrentRow, 
                currentTableTotalRows);
        
        if(doUpdate) update();
    }
    
    // Calculate the working dots and print the status
    public void update() {
        if(!this.enabled) return;
        
        long thisUpdateCheck = System.currentTimeMillis(); // % 1000;

        if(updateCheck > 0 && (thisUpdateCheck - updateCheck) < updateIntervalMillis)
            return;
        
        updateCheck = thisUpdateCheck;
        
        TextStringBuilder output = new TextStringBuilder();
        //StringUtils.
        
        output.append("\r%s%s", status, StringUtils.repeat(".", dotMarkCount));
        
        //int padLength = Math.max(status.length(), totalRows)
        
        resetCharsLength = Math.max(resetCharsLength, output.toString().length() + maxDotMarks - dotMarkCount);
        
        System.out.print(StringUtils.rightPad(output.toString(), resetCharsLength));
        System.out.flush();

        if(dotMarkCount >= maxDotMarks)
            dotMarkCount = 0;
        else
            dotMarkCount++;
    }
    
    public boolean calculateAllRows() {
        if(!this.enabled) return false;
        
        this.update();
        this.totalRows = 0;
        boolean result = false;
        final String methodName = "calculateAllRows";
        try {
            Set<String> tableNames = db.getTableNames();
            //Json.createArrayBuilder().
            //List<JsonObject> jsonTables;
            
            tableNames.forEach((tableName) -> {
                try {
                    Table table = db.getTable(tableName);
                    this.totalRows += table.getRowCount();

                } catch(IOException e) {
                    //lastError.add(String.format("Could not load table '%s'", tableName));
                    AccessConverter.Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
                
                this.update();
            });
            
            result = true;
        } catch(IOException e) {
            //lastError.add("Could not fetch tables from the database");
            AccessConverter.Error("Could not fetch tables from the database", e, methodName);
        }
        
        return result;
    }
}
