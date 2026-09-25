package io.lytrax.accessconverter.value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.model.AccessType;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AccessValuesTest {

    @ParameterizedTest
    @EnumSource(AccessType.class)
    void nullStaysNull(AccessType type) {
        assertThat(AccessValues.canonical(type, null)).isNull();
    }

    @Test
    void byteIsUnsigned() {
        assertThat(AccessValues.canonical(AccessType.BYTE, (byte) 0)).isEqualTo(0);
        assertThat(AccessValues.canonical(AccessType.BYTE, (byte) 127)).isEqualTo(127);
        assertThat(AccessValues.canonical(AccessType.BYTE, (byte) -56)).isEqualTo(200);
        assertThat(AccessValues.canonical(AccessType.BYTE, (byte) -1)).isEqualTo(255);
    }

    @Test
    void byteRoundTripsForIndexLookups() {
        assertThat(AccessValues.toJackcess(AccessType.BYTE, 200)).isEqualTo((byte) -56);
        assertThat(AccessValues.toJackcess(AccessType.LONG, 200)).isEqualTo(200);
    }

    @Test
    void numbersKeepTheirJavaTypeAndScale() {
        assertThat(AccessValues.canonical(AccessType.INT, (short) -3)).isEqualTo((short) -3);
        assertThat(AccessValues.canonical(AccessType.BIG_INT, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        BigDecimal money = new BigDecimal("123456789012345.1234");
        assertThat(AccessValues.canonical(AccessType.MONEY, money)).isSameAs(money);
        assertThat(AccessValues.canonical(AccessType.FLOAT, 2583.2092f)).isEqualTo(2583.2092f);
    }

    @Test
    void datesAreLocal() {
        LocalDateTime t = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_700);
        assertThat(AccessValues.canonical(AccessType.EXT_DATE_TIME, t)).isEqualTo(t);
    }

    @Test
    void guidsAreUppercaseWithBraces() {
        assertThat(AccessValues.canonical(AccessType.GUID, "{6f1c2a3b-1111-2222-3333-444455556666}"))
                .isEqualTo("{6F1C2A3B-1111-2222-3333-444455556666}");
        assertThat(AccessValues.canonical(AccessType.AUTONUMBER_GUID, "6F1C2A3B-1111-2222-3333-444455556666"))
                .isEqualTo("{6F1C2A3B-1111-2222-3333-444455556666}");
        assertThatThrownBy(() -> AccessValues.canonical(AccessType.GUID, "not-a-guid"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void binaryAndOleKeepTheirBytes() {
        byte[] empty = new byte[0];
        assertThat(AccessValues.canonical(AccessType.BINARY, empty)).isSameAs(empty);
        assertThat(AccessValues.canonical(AccessType.OLE, new byte[] {1, 2}))
                .isEqualTo(new OleValue(new byte[] {1, 2}));
    }

    @Test
    void complexValuesAreTheirId() {
        assertThat(AccessValues.canonical(AccessType.ATTACHMENT, 42)).isEqualTo(new ComplexRef(42));
    }

    @Test
    void anUnexpectedJavaTypeFailsLoudly() {
        assertThatThrownBy(() -> AccessValues.canonical(AccessType.LONG, "12"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("LONG")
                .hasMessageContaining("java.lang.String");
    }
}
