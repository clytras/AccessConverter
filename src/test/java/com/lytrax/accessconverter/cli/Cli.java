package com.lytrax.accessconverter.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/** Test helper: runs the CLI in-process and captures its output (stdout decoded as UTF-8). */
record Cli(int exitCode, String out, String err) {

    static Cli run(String... args) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        StringWriter stderr = new StringWriter();
        int code = Main.run(stdout, StandardCharsets.UTF_8, new PrintWriter(stderr, true), args);
        return new Cli(code, stdout.toString(StandardCharsets.UTF_8), stderr.toString());
    }
}
