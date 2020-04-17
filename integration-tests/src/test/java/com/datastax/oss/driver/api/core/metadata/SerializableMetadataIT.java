package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(ParallelizableTests.class)
public class SerializableMetadataIT {

    private static final CcmRule ccmRule = CcmRule.getInstance();

    private static final SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

    @ClassRule
    public static final TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

    @Before
    public void setupKeyspace() {
        List<String> statements =
                ImmutableList.of(
                        "CREATE KEYSPACE IF NOT EXISTS numbers WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }",
                        "CREATE TABLE numbers.all (val int, doub int, str text, primary key (val,doub)) WITH CLUSTERING ORDER BY (doub DESC)",
                        "CREATE TYPE numbers.numbers_data (doub int, str text)",
                        "CREATE TABLE numbers.smaller (val int primary key, data numbers_data)",
                        "CREATE INDEX doub_idx on numbers.all (doub)",
                        "CREATE OR REPLACE FUNCTION numbers.avgState ( state tuple<int,bigint>, val int ) CALLED ON NULL INPUT RETURNS tuple<int,bigint> LANGUAGE java AS" +
                                " 'if (val !=null) { state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue()); } return state;'",
                        "CREATE OR REPLACE FUNCTION numbers.avgFinal ( state tuple<int,bigint> ) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS" +
                                " 'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);'",
                        "CREATE AGGREGATE IF NOT EXISTS numbers.average ( int ) SFUNC avgState STYPE tuple<int,bigint> FINALFUNC avgFinal INITCOND (0,0)",
                        "INSERT INTO numbers.all (val,doub,str) VALUES (1,2,'one')",
                        "INSERT INTO numbers.all (val,doub,str) VALUES (2,4,'two')",
                        "INSERT INTO numbers.all (val,doub,str) VALUES (3,6,'three')",
                        "CREATE MATERIALIZED VIEW IF NOT EXISTS numbers.big AS SELECT FROM numbers.all" +
                                " WHERE val is not null and doub is not null and val > 2" +
                                " PRIMARY KEY (val,doub)"
                );

        statements.stream().forEach((stmt) -> sessionRule.session().execute(stmt));
    }

    @Test
    public void should_contain_serializable_metadata() {

        Optional<KeyspaceMetadata> ksOption = sessionRule.session().getMetadata().getKeyspace("numbers");
        assertTrue(ksOption.isPresent());
        KeyspaceMetadata ks = ksOption.get();
        assertTrue(ks instanceof DefaultKeyspaceMetadata);

        /* Validate that the keyspace metadata is fully populated */
        assertFalse(ks.getUserDefinedTypes().isEmpty());
        assertFalse(ks.getTables().isEmpty());
        assertFalse(ks.getViews().isEmpty());
        assertFalse(ks.getFunctions().isEmpty());
        assertFalse(ks.getAggregates().isEmpty());

        Optional<TableMetadata> tableOption = ks.getTable("all");
        assertTrue(tableOption.isPresent());
        TableMetadata table = tableOption.get();
        assertTrue(table instanceof DefaultTableMetadata);

        /* Validate that the table metadata is fully populated */
        assertFalse(table.getPartitionKey().isEmpty());
        assertFalse(table.getClusteringColumns().isEmpty());
        assertFalse(table.getColumns().isEmpty());
        // TODO: What kind of objects can be set for options here?  How do we guarantee they're serializable?
        assertFalse(table.getOptions().isEmpty());
        assertFalse(table.getIndexes().isEmpty());

        SerializationUtils.roundtrip((DefaultKeyspaceMetadata)ks);
    }
}
