package io.lytrax.accessconverter.source;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.HashMap;
import java.util.Map;

/**
 * A single-byte code page decoded the way Windows, and so Access, decodes it. The JDK leaves a few bytes of the
 * Windows code pages undefined and turns them into U+FFFD; Windows maps every byte. An undefined byte in 0x80-0x9F
 * becomes the C1 control of the same value (0x81, 0x8D, 0x8F, 0x90 and 0x9D in windows-1252), and the few above 0x9F
 * in 874, 1253, 1255 and 1257 become the characters in {@link #WINDOWS}, mostly in the private use area: measured
 * with Windows' own {@code MultiByteToWideChar} on Windows 11 (3.0.1), and 1253's 0xAA confirmed in Access 97 itself,
 * which returns U+F8F9 for it. Every byte therefore decodes, and encodes back to itself.
 *
 * <p>It keeps the JDK charset's name, so it is equal to it; it differs only on bytes the JDK can't decode.
 */
final class WindowsSingleByte extends Charset {
    /** U+FFFD, what a byte the code page does not define decodes to. */
    private static final char REPLACEMENT = (char) 0xFFFD;

    /** Windows' characters for the bytes the JDK leaves undefined above 0x9F: byte, character, byte, character, … */
    private static final Map<String, int[]> WINDOWS = Map.of(
            "x-windows-874",
            new int[] {
                0xDB, 0xF8C1, 0xDC, 0xF8C2, 0xDD, 0xF8C3, 0xDE, 0xF8C4, 0xFC, 0xF8C5, 0xFD, 0xF8C6, 0xFE, 0xF8C7, 0xFF,
                0xF8C8
            },
            "windows-1253",
            new int[] {0xAA, 0xF8F9, 0xD2, 0xF8FA, 0xFF, 0xF8FB},
            "windows-1255",
            new int[] {
                0xCA, 0x05BA, 0xD9, 0xF88D, 0xDA, 0xF88E, 0xDB, 0xF88F, 0xDC, 0xF890, 0xDD, 0xF891, 0xDE, 0xF892, 0xDF,
                0xF893, 0xFB, 0xF894, 0xFC, 0xF895, 0xFF, 0xF896
            },
            "windows-1257",
            new int[] {0xA1, 0xF8FC, 0xA5, 0xF8FD});

    private final Charset base;
    private final char[] decode = new char[256];
    private final Map<Character, Byte> encode = new HashMap<>();

    private WindowsSingleByte(Charset base) {
        super(base.name(), null);
        this.base = base;
        for (int b = 0; b < 256; b++) {
            String s = new String(new byte[] {(byte) b}, base);
            char c = s.length() == 1 ? s.charAt(0) : REPLACEMENT;
            if (c == REPLACEMENT && b >= 0x80 && b <= 0x9F) {
                c = (char) b;
            }
            decode[b] = c;
        }
        int[] windows = WINDOWS.getOrDefault(base.name(), new int[0]);
        for (int i = 0; i < windows.length; i += 2) {
            if (decode[windows[i]] == REPLACEMENT) {
                decode[windows[i]] = (char) windows[i + 1];
            }
        }
        for (int b = 0; b < 256; b++) {
            char c = decode[b];
            if (c != REPLACEMENT) {
                encode.putIfAbsent(c, (byte) b);
            }
        }
    }

    /** The charset to decode Access 97 text with: {@code charset} itself unless it is single-byte. */
    static Charset of(Charset charset) {
        if (charset instanceof WindowsSingleByte || !isSingleByte(charset)) {
            return charset;
        }
        return new WindowsSingleByte(charset);
    }

    private static boolean isSingleByte(Charset charset) {
        return charset.canEncode()
                && charset.newEncoder().maxBytesPerChar() == 1f
                && charset.newDecoder().maxCharsPerByte() == 1f;
    }

    @Override
    public boolean contains(Charset cs) {
        return cs.name().equals(name()) || base.contains(cs);
    }

    @Override
    public CharsetDecoder newDecoder() {
        return new CharsetDecoder(this, 1f, 1f) {
            @Override
            protected CoderResult decodeLoop(ByteBuffer in, CharBuffer out) {
                while (in.hasRemaining()) {
                    char c = decode[in.get(in.position()) & 0xFF];
                    if (c == REPLACEMENT) {
                        return CoderResult.unmappableForLength(1);
                    }
                    if (!out.hasRemaining()) {
                        return CoderResult.OVERFLOW;
                    }
                    in.position(in.position() + 1);
                    out.put(c);
                }
                return CoderResult.UNDERFLOW;
            }
        };
    }

    @Override
    public CharsetEncoder newEncoder() {
        return new CharsetEncoder(this, 1f, 1f, new byte[] {'?'}) {
            @Override
            protected CoderResult encodeLoop(CharBuffer in, ByteBuffer out) {
                while (in.hasRemaining()) {
                    char c = in.get(in.position());
                    Byte b = encode.get(c);
                    if (b == null) {
                        if (Character.isHighSurrogate(c)) {
                            if (in.remaining() < 2) {
                                return CoderResult.UNDERFLOW;
                            }
                            return Character.isLowSurrogate(in.get(in.position() + 1))
                                    ? CoderResult.unmappableForLength(2)
                                    : CoderResult.malformedForLength(1);
                        }
                        return Character.isLowSurrogate(c)
                                ? CoderResult.malformedForLength(1)
                                : CoderResult.unmappableForLength(1);
                    }
                    if (!out.hasRemaining()) {
                        return CoderResult.OVERFLOW;
                    }
                    in.position(in.position() + 1);
                    out.put(b);
                }
                return CoderResult.UNDERFLOW;
            }
        };
    }
}
