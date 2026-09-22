package com.lytrax.accessconverter.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

final class Checksums {
    private Checksums() {}

    /** Lowercase hex SHA-256 of a file's content. */
    static String sha256(Path file) throws IOException {
        MessageDigest digest = newDigest();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = Files.newInputStream(file)) {
            for (int n; (n = in.read(buffer)) != -1; ) {
                digest.update(buffer, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** True when the file exists with exactly this size and SHA-256. */
    static boolean matches(Path file, long size, String sha256) throws IOException {
        return Files.isRegularFile(file)
                && Files.size(file) == size
                && sha256(file).equals(sha256);
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every JDK provides SHA-256", e);
        }
    }
}
