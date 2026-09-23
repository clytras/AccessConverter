package com.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import org.junit.jupiter.api.Test;

class CodePagesTest {

    @Test
    void everyMappedCodePageExistsInThisRuntime() {
        // A runtime image without jdk.charsets would lose the CJK and Thai code pages: this catches it
        CodePages.mappings()
                .forEach((codePage, name) -> assertThat(CodePages.charset(codePage))
                        .as("code page %d -> %s", codePage, name)
                        .contains(Charset.forName(name)));
    }

    @Test
    void windowsAnsiAndEastAsianCodePages() {
        assertThat(CodePages.charset(1252)).contains(Charset.forName("windows-1252"));
        assertThat(CodePages.charset(1253)).contains(Charset.forName("windows-1253"));
        assertThat(CodePages.charset(932)).contains(Charset.forName("windows-31j"));
        assertThat(CodePages.charset(936)).contains(Charset.forName("GBK"));
        assertThat(CodePages.charset(949)).contains(Charset.forName("x-windows-949"));
        assertThat(CodePages.charset(950)).contains(Charset.forName("x-windows-950"));
        assertThat(CodePages.charset(874)).contains(Charset.forName("x-windows-874"));
    }

    @Test
    void unknownCodePagesHaveNoMapping() {
        assertThat(CodePages.charset(0)).isEmpty();
        assertThat(CodePages.charset(4242)).isEmpty();
    }
}
