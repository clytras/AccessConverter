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

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
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
        if (!this.enabled) {
            return;
        }

        System.out.printf("\r%s", StringUtils.repeat(" ", status.length() + dotMarkCount));
        System.out.print('\r');
        System.out.flush();
    }

    public void startTable(Table tbl) {
        if (!this.enabled) {
            return;
        }

        this.currentTable = tbl.getName();
        this.currentTableTotalRows = tbl.getRowCount();
        this.currentTableCurrentRow = 0;
        progress(true);
    }

    public void endTable() {
        if (!this.enabled) {
            return;
        }

        if (currentTableCurrentRow != currentTableTotalRows) {
            currentRow += currentTableTotalRows - currentTableCurrentRow;
        }

        progress(true);
    }

    public void step() {
        step(1);
    }

    public void step(int steps) {
        if (!this.enabled) {
            return;
        }

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

        if (doUpdate) {
            update();
        }
    }

    // Calculate the working dots and print the status
    public void update() {
        if (!this.enabled) {
            return;
        }

        long thisUpdateCheck = System.currentTimeMillis();

        if (updateCheck > 0 && (thisUpdateCheck - updateCheck) < updateIntervalMillis) {
            return;
        }

        updateCheck = thisUpdateCheck;

        TextStringBuilder output = new TextStringBuilder();

        output.append("\r%s%s", status, StringUtils.repeat(".", dotMarkCount));

        resetCharsLength = Math.max(resetCharsLength, output.toString().length() + maxDotMarks - dotMarkCount);

        System.out.print(StringUtils.rightPad(output.toString(), resetCharsLength));
        System.out.flush();

        if (dotMarkCount >= maxDotMarks) {
            dotMarkCount = 0;
        } else {
            dotMarkCount++;
        }
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
