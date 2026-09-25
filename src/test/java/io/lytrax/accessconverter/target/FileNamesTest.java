package io.lytrax.accessconverter.target;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FileNamesTest {

    @Test
    void anOrdinaryNameIsKept() {
        FileNames names = new FileNames();
        assertThat(names.allocate("Order Details", ".ndjson")).isEqualTo("Order Details.ndjson");
        assertThat(names.allocate("Πελάτες", ".ndjson")).isEqualTo("Πελάτες.ndjson");
    }

    @Test
    void directoryPartsAndCharactersSomeSystemRejectsAreRemoved() {
        FileNames names = new FileNames();
        assertThat(names.allocate("..\\..\\x.txt", "")).isEqualTo("x.txt");
        assertThat(names.allocate("../../etc/passwd", "")).isEqualTo("passwd");
        assertThat(names.allocate("a<b>c:d\"e|f?g*h\u0001", ".ndjson")).isEqualTo("a_b_c_d_e_f_g_h_.ndjson");
        assertThat(names.allocate("trailing dots. . ", ".ndjson")).isEqualTo("trailing dots.ndjson");
        assertThat(names.allocate(".hidden", ".ndjson")).isEqualTo("_hidden.ndjson");
        assertThat(names.allocate("", ".ndjson")).isEqualTo("_.ndjson");
    }

    @Test
    void windowsDeviceNamesAreNeverUsed() {
        FileNames names = new FileNames();
        assertThat(names.allocate("CON", ".ndjson")).isEqualTo("_CON.ndjson");
        assertThat(names.allocate("nul.txt", "")).isEqualTo("_nul.txt");
        assertThat(names.allocate("COM1", ".ndjson")).isEqualTo("_COM1.ndjson");
        assertThat(names.allocate("CONTACTS", ".ndjson")).isEqualTo("CONTACTS.ndjson");
    }

    @Test
    void namesThatOneFileSystemWouldTakeForTheSameFileGetASuffix() {
        FileNames names = new FileNames();
        assertThat(names.allocate("a/b", ".ndjson")).isEqualTo("b.ndjson");
        assertThat(names.allocate("B", ".ndjson")).isEqualTo("B-2.ndjson");
        // é composed and decomposed: one file on macOS
        assertThat(names.allocate("café", ".ndjson")).isEqualTo("café.ndjson");
        assertThat(names.allocate("café", ".ndjson")).isEqualTo("café-2.ndjson");
    }

    @Test
    void longNamesAreCutWithoutSplittingACharacter() {
        String name = "x".repeat(FileNames.MAX_LENGTH - 12) + "😀😀😀😀😀😀";
        String allocated = new FileNames().allocate(name, ".ndjson");

        assertThat(allocated.length()).isLessThanOrEqualTo(FileNames.MAX_LENGTH);
        assertThat(allocated).endsWith(".ndjson");
        assertThat(allocated.codePoints().noneMatch(c -> Character.isSurrogate((char) c)))
                .isTrue();
    }
}
