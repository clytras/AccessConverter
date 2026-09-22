package com.lytrax.accessconverter.fixtures;

import static com.lytrax.accessconverter.fixtures.GeneratedFixtureTest.localTableNames;
import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tier C smoke test: each maintainer sample opens with the table count given in 01, "Maintainer samples". */
@LocalSamples
class LocalSampleTest {

    @ParameterizedTest
    @CsvSource({
        "NORTHWIND_2007,          21",
        "ALL_TYPES,               4",
        "ALL_TYPES_MDB,           1",
        "TEST_DB,                 1",
        "MARKET_BASKET,           23",
        "FAV_DATABASE,            14",
        "HOTEL_MANAGEMENT_SYSTEM, 3",
    })
    void opensWithTheAuditedTableCount(LocalSample sample, int tables) throws IOException {
        try (Database db = sample.open()) {
            assertThat(localTableNames(db)).hasSize(tables);
        }
    }
}
