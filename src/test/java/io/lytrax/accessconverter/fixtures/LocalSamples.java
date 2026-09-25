package io.lytrax.accessconverter.fixtures;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;

/** Marks tests that need {@link LocalSample} files. Surefire excludes the tag unless {@code -Plocal-samples}. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Tag(LocalSamples.TAG)
public @interface LocalSamples {
    String TAG = "local-samples";
}
