package io.lytrax.accessconverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

/**
 * Guards the surefire locale matrix in pom.xml: each execution must really run under its locale and time zone,
 * otherwise a test that passes only under {@code Locale.ROOT}-like defaults would slip through.
 */
class TestEnvironmentTest {

    @Test
    void runsUnderTheConfiguredLocaleAndTimeZone() {
        String locale = System.getProperty("expected.locale");
        String zone = System.getProperty("expected.timezone");
        assumeTrue(locale != null && zone != null, "not run by Maven surefire");

        assertThat(Locale.getDefault().toLanguageTag()).isEqualTo(locale);
        assertThat(Locale.getDefault(Locale.Category.FORMAT).toLanguageTag()).isEqualTo(locale);
        assertThat(TimeZone.getDefault().getID()).isEqualTo(zone);
        if (locale.equals("el-GR")) {
            assertThat(DecimalFormatSymbols.getInstance().getDecimalSeparator()).isEqualTo(',');
        }
    }
}
