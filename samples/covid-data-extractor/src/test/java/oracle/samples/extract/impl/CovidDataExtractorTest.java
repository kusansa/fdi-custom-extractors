package oracle.samples.extract.impl;

import oracle.apps.bi.custom.extractservice.extract.ExtractDataStore;
import oracle.apps.bi.custom.extractservice.extract.ExtractorConfig;
import oracle.apps.bi.custom.extractservice.extract.dataExtract.TaskExecutionSummary;
import oracle.apps.bi.custom.extractservice.model.DataStoreColumnMeta;
import oracle.apps.bi.custom.extractservice.model.DataStoreInfo;
import oracle.apps.bi.custom.extractservice.model.DataStoreMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test class for validating extractor implementations such as {@code CovidDataExtractorImpl}.
 * <p>
 * This test dynamically loads and tests a specific extractor implementation based on configuration
 * provided in the {@code extractor_test.properties} file. It validates the extractor’s lifecycle,
 * including initialization, metadata extraction, datastore queries, and version information.
 * <p>
 * The tests rely on reflection to avoid compile-time dependencies on specific extractor classes,
 * allowing this test to be reused for multiple extractor implementations.
 *
 * <h2>Test Flow</h2>
 * <ol>
 *     <li>Loads test configuration properties.</li>
 *     <li>Dynamically instantiates the extractor class defined in the properties file.</li>
 *     <li>Invokes initialization with a mock {@link ExtractorConfig}.</li>
 *     <li>Verifies core extractor functions such as connection, metadata retrieval, and data querying.</li>
 * </ol>
 *
 * <p><b>Note:</b> This class assumes the existence of a valid extractor implementation and
 * a properly configured {@code extractor_test.properties} file in the test resources.</p>
 *
 * @author Oracle
 * @since 24.5.0
 */
// Please remove below line to enable test cases
@Disabled("Temporarily disabling all tests in this class")
public class CovidDataExtractorTest {

    /** The dynamically instantiated extractor implementation under test. */
    private Object extractor;

    /** Properties loaded from {@code extractor_test.properties}. */
    private Properties props;

    /**
     * Initializes test configuration before each test execution.
     * <p>
     * Loads configuration from {@code extractor_test.properties}, dynamically resolves
     * the extractor class, creates an instance, and calls its {@code init(ExtractorConfig)} method.
     *
     * @throws Exception if the configuration file is missing, unreadable, or the extractor cannot be instantiated.
     */
    @BeforeEach
    void setup() throws Exception {
        props = new Properties();
        try (InputStream input = CovidDataExtractorTest.class.getClassLoader()
                .getResourceAsStream("extractor_test.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find extractor_test.properties");
            }
            props.load(input);
            String packageName = CovidDataExtractorTest.class.getPackageName();
            String fullClassName = packageName + "." + props.getProperty("className");
            props.setProperty("className", fullClassName);
            System.out.println("Testing Class: " + fullClassName);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load extractor_test.properties", ex);
        }

        String className = props.getProperty("className");
        if (className == null || className.isEmpty()) {
            throw new IllegalArgumentException("className must be defined in extractor_test.properties");
        }

        Class<?> clazz = Class.forName(className);
        extractor = clazz.getDeclaredConstructor().newInstance();
        clazz.getMethod("init", ExtractorConfig.class)
                .invoke(extractor, buildExtractorTestConfig());
    }

    /**
     * Builds an {@link ExtractorConfig} instance for use in extractor initialization.
     * <p>
     * The configuration includes properties loaded from the test file, environment/system
     * variable substitution, and defaults for extract output directory and job identifiers.
     *
     * @return an initialized {@link ExtractorConfig} containing test configuration values.
     */
    private ExtractorConfig buildExtractorTestConfig() {
        Map<String, String> config = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            String value = props.getProperty(name);
            if (value != null && value.startsWith("${") && value.endsWith("}")) {
                String key = value.substring(2, value.length() - 1);
                value = Optional.ofNullable(System.getProperty(key))
                        .orElse(System.getenv(key));
            }
            config.put(name, value);
        }

        config.putIfAbsent("EXTRACT_OUTPUT_DIR", System.getProperty("java.io.tmpdir"));
        Map<String, Object> adhoc = new HashMap<>();

        return new ExtractorConfig() {
            @Override
            public Map<String, String> getConfig() {
                return config;
            }

            @Override
            public Map<String, Object> getAdhocConfig() {
                return adhoc;
            }

            @Override
            public Long getJobId() {
                return 1L;
            }

            @Override
            public String getRequestId() {
                return "req-live-1";
            }
        };
    }

    /**
     * Validates that the extractor returns the correct source type.
     * <p>
     * Compares the extractor's reported source type against the expected value
     * defined in the properties file.
     *
     * @throws Exception if reflection-based invocation fails.
     */
    @Test
    void testGetSourceType() throws Exception {
        String expectedSourceType = props.getProperty("sourceType");
        assertNotNull(expectedSourceType, "sourceType must be defined in extractor.properties");
        String actual = (String) extractor.getClass().getMethod("getSourceType").invoke(extractor);
        assertEquals(expectedSourceType, actual);
    }

    /**
     * Verifies that the extractor’s connection validation logic executes without error.
     * <p>
     * Ensures that {@code verifyConnection()} does not throw exceptions for a valid configuration.
     *
     * @throws Exception if the reflective invocation fails.
     */
    @Test
    void testVerifyConnection() throws Exception {
        TaskExecutionSummary summary = new TaskExecutionSummary();
        assertDoesNotThrow(() ->
                extractor.getClass().getMethod("verifyConnection", ExtractorConfig.class, TaskExecutionSummary.class)
                        .invoke(extractor, buildExtractorTestConfig(), summary)
        );
    }

    /**
     * Tests the extractor’s version metadata retrieval.
     * <p>
     * Ensures that the returned version map is not null, non-empty, and that all values
     * are valid numeric representations.
     *
     * @throws Exception if reflective method invocation fails.
     */
    @Test
    void testGetVersion() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, String> versionMap = (Map<String, String>) extractor.getClass()
                .getMethod("getVersion").invoke(extractor);

        assertNotNull(versionMap);
        assertFalse(versionMap.isEmpty());
        versionMap.forEach((key, value) ->
                assertDoesNotThrow(() -> Double.parseDouble(value),
                        "Value for key '" + key + "' should be a valid double, but was: " + value)
        );
    }

    /**
     * Tests that the extractor can enumerate available datastores.
     * <p>
     * Ensures that the iterator returned by {@code extractDataStores()} yields at least one valid
     * {@link DataStoreInfo} and that each datastore name is non-null.
     *
     * @throws Exception if reflective invocation fails.
     */
    @Test
    void testExtractDataStores() throws Exception {
        @SuppressWarnings("unchecked")
        Iterator<DataStoreInfo> dsIterator =
                (Iterator<DataStoreInfo>) extractor.getClass().getMethod("extractDataStores").invoke(extractor);

        assertTrue(dsIterator.hasNext(), "No DataStoreInfo returned from live repo");
        while (dsIterator.hasNext()) {
            DataStoreInfo dsInfo = dsIterator.next();
            assertNotNull(dsInfo.getName());
            System.out.println("Found datastore: " + dsInfo.getName());
        }
    }

    /**
     * Validates that each datastore exposes valid column metadata.
     * <p>
     * Ensures that {@code extractDataStoreColumns()} returns one or more {@link DataStoreMeta}
     * instances with valid column definitions, including supported data types and non-empty column lists.
     *
     * @throws Exception if reflective invocation fails.
     */
    @Test
    void testExtractDataStoreColumns() throws Exception {
        @SuppressWarnings("unchecked")
        Iterator<DataStoreMeta> metaIterator =
                (Iterator<DataStoreMeta>) extractor.getClass().getMethod("extractDataStoreColumns")
                        .invoke(extractor);

        assertTrue(metaIterator.hasNext());
        List<String> dataTypes = Arrays.asList("VARCHAR2", "TIMESTAMP", "BOOLEAN", "NUMBER");

        while (metaIterator.hasNext()) {
            DataStoreMeta meta = metaIterator.next();
            assertNotNull(meta.getName());
            assertFalse(meta.getColumns().isEmpty());
            System.out.println("Columns for datastore " + meta.getName() + ": " +
                    meta.getColumns().stream().map(DataStoreColumnMeta::getName).toList());
            meta.getColumns().forEach(col -> {
                assertNotNull(col.getName());
                assertNotNull(col.getDataType());
                assertTrue(dataTypes.contains(col.getDataType()));
            });
        }
    }

    /**
     * Tests querying live datastore content.
     * <p>
     * Iterates over available datastore metadata, constructs an {@link ExtractDataStore} for each,
     * and validates that the extractor returns a non-null {@link ResultSet} with a valid result set.
     *
     * @throws Exception if reflective invocation fails.
     */
    @Test
    void testQueryData() throws Exception {
        @SuppressWarnings("unchecked")
        Iterator<DataStoreMeta> metaIterator =
                (Iterator<DataStoreMeta>) extractor.getClass().getMethod("extractDataStoreColumns")
                        .invoke(extractor);

        if (!metaIterator.hasNext()) {
            fail("No datastore found.");
            return;
        }

        while (metaIterator.hasNext()) {
            DataStoreMeta dsMeta = metaIterator.next();

            ExtractDataStore ds = new ExtractDataStore();
            ds.setName(dsMeta.getName());
            List<String> columnOrder = dsMeta.getColumns().stream()
                    .map(DataStoreColumnMeta::getName).collect(Collectors.toList());
            ds.setColumnOrder(columnOrder);

            Map<String, String> colDataTypeMap = dsMeta.getColumns().stream()
                    .collect(Collectors.toMap(DataStoreColumnMeta::getName, DataStoreColumnMeta::getDataType));
            ds.setColumnToDataTypeMap(colDataTypeMap);

            System.out.println("Testing datastore: " + ds.getName());

            ResultSet result  = (ResultSet)
                    extractor.getClass().getMethod("queryData", ExtractDataStore.class)
                            .invoke(extractor, ds);

            assertNotNull(result);
            System.out.println("QueryData worked fine for datastore: " + ds.getName());
        }
    }
}
