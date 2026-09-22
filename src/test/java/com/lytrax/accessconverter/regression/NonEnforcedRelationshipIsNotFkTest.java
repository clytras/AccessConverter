package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Extraction;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.ForeignKeyModel.Status;
import com.lytrax.accessconverter.report.IssueCode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * F-31: a relationship without "Enforce Referential Integrity" is a join line only; Access allows orphans in it.
 * v2 exported it as a foreign key, which failed on the orphans. v3 marks it NOT_ENFORCED and never emits it.
 */
class NonEnforcedRelationshipIsNotFkTest {
    private final Extraction schemaFidelity = Extraction.of(GeneratedFixture.SCHEMA_FIDELITY.path());

    @Test
    void theJoinLineIsNotAForeignKey() {
        ForeignKeyModel r = schemaFidelity.relationship("SuppliersProducts");
        assertThat(r.enforced()).isFalse();
        assertThat(r.status()).isEqualTo(Status.NOT_ENFORCED);
        assertThat(r.parentKey()).isNull();
        assertThat(schemaFidelity.issues(IssueCode.FK_SKIPPED_NOT_ENFORCED))
                .singleElement()
                .satisfies(i -> assertThat(i.object()).isEqualTo("SuppliersProducts"));
    }

    @Test
    void enforcedRelationshipsAreCandidatesWithTheirActions() {
        assertThat(schemaFidelity.relationship("CustomersOrders")).satisfies(r -> {
            assertThat(r.status()).isEqualTo(Status.EMIT);
            assertThat(r.parentKey()).isEqualTo("PrimaryKey");
            assertThat(r.onUpdate()).isEqualTo(Action.CASCADE);
            assertThat(r.onDelete()).isEqualTo(Action.CASCADE);
        });
        assertThat(schemaFidelity.relationship("ShippersOrders").onDelete()).isEqualTo(Action.SET_NULL);
        assertThat(schemaFidelity.relationship("OrdersOrder Details").onUpdate())
                .isEqualTo(Action.NO_ACTION);
        assertThat(schemaFidelity.relationship("ProductsOrder Details").onDelete())
                .isEqualTo(Action.NO_ACTION);
    }

    @Nested
    @LocalSamples
    class Samples {
        @Test
        void allTypesOneToOneJoinLine() {
            Extraction allTypes = Extraction.of(LocalSample.ALL_TYPES.path());
            assertThat(allTypes.relationship("testPrimaryKeystestTableRelation"))
                    .satisfies(r -> {
                        assertThat(r.oneToOne()).isTrue();
                        assertThat(r.status()).isEqualTo(Status.NOT_ENFORCED);
                    });
        }

        @Test
        void northwindRelationshipsAreAllEnforced() {
            Extraction northwind = Extraction.of(LocalSample.NORTHWIND_2007.path());
            assertThat(northwind.model().relationships())
                    .hasSize(19)
                    .allSatisfy(r -> assertThat(r.status()).isEqualTo(Status.EMIT));
        }
    }
}
