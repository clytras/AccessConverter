package com.lytrax.accessconverter.source;

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
 * Windows code pages undefined (0x81, 0x8D, 0x8F, 0x90 and 0x9D in windows-1252) and turns them into U+FFFD;
 * Windows maps an undefined byte in 0x80-0x9F to the C1 control of the same value. That is the only change: bytes
 * the JDK maps keep their mapping, and the few undefined bytes above 0x9F (in 1253, 1255, 1257 and 874, which
 * Windows sends to its private use area) still decode to U+FFFD, which the profiler counts.
 *
 * <p>It keeps the JDK charset's name, so it is equal to it; it differs only on bytes the JDK can't decode.
 */
final class WindowsSingleByte extends Charset {
    /** U+FFFD, what a byte the code page does not define decodes to. */
    private static final char REPLACEMENT = (char) 0xFFFD;

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
