package oracle.samples.extract.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.micrometer.core.instrument.util.StringUtils;
import oracle.apps.bi.custom.extractservice.exception.CustomExtractException;
import oracle.apps.bi.custom.extractservice.extract.CustomExtractor;
import oracle.apps.bi.custom.extractservice.extract.ExtractDataStore;
import oracle.apps.bi.custom.extractservice.extract.ExtractorConfig;
import oracle.apps.bi.custom.extractservice.extract.dataExtract.TaskExecutionSummary;
import oracle.apps.bi.custom.extractservice.extract.util.ExtractorUtil;
import oracle.apps.bi.custom.extractservice.model.DataStoreColumnMeta;
import oracle.apps.bi.custom.extractservice.model.DataStoreInfo;
import oracle.apps.bi.custom.extractservice.model.DataStoreMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * {@code CovidDataExtractorImpl} is a sample implementation of the {@link CustomExtractor} interface
 * designed to fetch and process COVID-19 analytics data from a public GitHub repository.
 * <p>
 * <b>Key Features:</b>
 * <ul>
 *     <li>Downloads CSV files containing COVID-19 case data from GitHub.</li>
 *     <li>Supports dynamic discovery of available datastores for augmentation into a data warehouse.</li>
 *     <li>Maps COVID data columns to Oracle-compatible data types.</li>
 *     <li>Provides ResultSet-like access to the downloaded data through {@link ResultSet}.</li>
 *     <li>Supports extraction of column metadata and primary key mapping for each datastore.</li>
 *     <li>Automatically extracts ZIP files and manages temporary data storage.</li>
 * </ul>
 * <p>
 * This class demonstrates a prototype connector suitable for testing, educational purposes,
 * and as a reference for building custom extractors in enterprise BI environments.
 *
 * <h2>Class Workflow</h2>
 * <ol>
 *     <li>Initialize the extractor with {@link #init(ExtractorConfig)}, which sets up properties and HTTP client.</li>
 *     <li>Determine available datastores via {@link #extractDataStores()}.</li>
 *     <li>Fetch and map columns for each datastore using {@link #extractDataStoreColumns()}.</li>
 *     <li>Download the latest CSV data files for a datastore using {@link #queryData(ExtractDataStore)}.</li>
 *     <li>Return a {@link ResultSet}-compatible wrapper for downstream processing.</li>
 *     <li>Optionally unzip any compressed files and clean up temporary data.</li>
 * </ol>
 *
 * <h2>Proxy Configuration</h2>
 * <p>
 * If system properties {@code APP_PROXY_HOST} and {@code APP_PROXY_PORT} are defined, the extractor
 * will route HTTP requests through the specified proxy server using {@link RestTemplate}.
 * </p>
 *
 * <h2>Column and Data Type Mapping</h2>
 * <p>
 * The class maintains a static mapping of common COVID-19 data columns (e.g., "Confirmed", "Deaths",
 * "Recovered") to Oracle-supported numeric or timestamp types. Primary key columns for each datastore
 * are also maintained to ensure proper data integration.
 * </p>
 *
 * <h2>ResultSet Wrapper</h2>
 * <p>
 * The {@link ResultSet} inner class wraps a CSV-backed {@link ResultSet} and
 * tracks missing columns, enabling Extract Service to validate data completeness.
 * </p>
 *
 * <h2>Example Usage</h2>
 * <pre>
 *     ExtractorConfig config = ...;
 *     CovidDataExtractorImpl extractor = new CovidDataExtractorImpl();
 *     extractor.init(config);
 *     Iterator&lt;DataStoreInfo&gt; stores = extractor.extractDataStores();
 *     while (stores.hasNext()) {
 *         ExtractDataStore ds = ...;
 *         ExtractQueryResult result = extractor.queryData(ds);
 *         ResultSet rs = result.getResultSet();
 *         ...
 *     }
 * </pre>
 *
 * <p>Note: This implementation is intended for prototype/testing scenarios. Production-grade
 * extractors may require additional error handling, performance optimizations, and security considerations.</p>
 *
 * @author Oracle
 * @since 24.5.0
 */
@Component
@Scope("prototype")
public class CovidDataExtractorImpl implements CustomExtractor {

    /**
     * Constant source type identifier for this extractor.
     */
    private static final String EXTRACT_TYPE = "COVIDDATA";

    /**
     * REST client for GitHub API requests, optionally configured with proxy.
     */
    private RestTemplate restTemplate;

    /**
     * Proxy host read from system properties if configured.
     */
    private static final String PROXY_SERVER_HOST = System.getProperty("APP_PROXY_HOST");

    /**
     * Proxy port read from system properties if configured.
     */
    private static final String PROXY_SERVER_PORT = System.getProperty("APP_PROXY_PORT");

    /**
     * Temporary list of downloaded data files for processing.
     */
    protected List<String> dataFiles = new ArrayList<>();

    /**
     * List of datastore names to exclude from extraction.
     */
    List<String> blacklistDatastore = Arrays.asList("csse_covid_19_time_series");

    /**
     * Logger for this class.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Mapping of COVID columns to Oracle data types.
     */
    private static Map<String, String> columnDataTypeMap;

    /**
     * Primary key mapping per datastore for data integration.
     */
    private static Map<String, String> dataStoreToPrimaryKeyColumnMap;

    /**
     * Properties loaded from ExtractorConfig for job-specific and ad-hoc parameters.
     */
    private Properties properties;

    static {
        columnDataTypeMap = new HashMap<>();
        columnDataTypeMap.put("Lat", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Long_", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Confirmed", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Deaths", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Recovered", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Active", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Incident_Rate", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Case_Fatality_Ratio", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Last_Update", ExtractorUtil.getTimestampDataType());
        columnDataTypeMap.put("FIPS", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Total_Test_Results", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("People_Tested", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Mortality_Rate", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("People_Hospitalized", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("UID", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Testing_Rate", ExtractorUtil.getNumericDataType());
        columnDataTypeMap.put("Hospitalization_Rate", ExtractorUtil.getNumericDataType());
    }

    static {
        dataStoreToPrimaryKeyColumnMap = new HashMap<>();
        dataStoreToPrimaryKeyColumnMap.put("csse_covid_19_daily_reports_us", "Province_State");
        dataStoreToPrimaryKeyColumnMap.put("csse_covid_19_daily_reports", "Combined_Key");
    }

    /**
     * Default constructor. Initializes the extractor and logs initialization.
     */
    public CovidDataExtractorImpl() {
        logger.info("Initializing CovidDataExtractorImpl");
    }


    /**
     * Initialize the connection properties. All properties configured for the source in Manage Connections UI will be available
     * in ExtractorConfig. Extract Service will invoke init method before any other call.
     *
     * @param extractorConfig all source connection properties
     */
    @Override
    public void init(ExtractorConfig extractorConfig) {
        if (!StringUtils.isBlank(PROXY_SERVER_HOST) && !StringUtils.isBlank(PROXY_SERVER_PORT)) {
            logger.info(
                    " getProxyConnector: proxy details are available for rest client, PROXY_HOST {}  PROXY_PORT {}",
                    PROXY_SERVER_HOST, PROXY_SERVER_PORT);
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(PROXY_SERVER_HOST, Integer.parseInt(PROXY_SERVER_PORT)));
            SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
            requestFactory.setProxy(proxy);
            restTemplate = new RestTemplate(requestFactory);
        } else {
            restTemplate = new RestTemplate();
        }
        properties = new Properties();
        CovidUtil.putAllMap(properties, extractorConfig.getConfig());
        CovidUtil.putAllMap(properties, extractorConfig.getAdhocConfig());
        properties.put("jobId", extractorConfig.getJobId());
        properties.put("requestId", extractorConfig.getRequestId());
    }


    /**
     * Unique identifier for the source. This is the source type to be provided when registering the custom connector.
     */
    @Override
    public String getSourceType() {
        return EXTRACT_TYPE;
    }

    /**
     * Constructs the download URL for a given datastore.
     * <p>
     * Queries the GitHub API to list files in the repository folder corresponding to the datastore.
     * Selects the latest CSV file based on the date in the file name.
     *
     * @param datastore the name of the datastore for which the download URL is required.
     * @return a list containing:
     * <ol>
     *     <li>Download URL of the latest CSV file.</li>
     *     <li>String representation of the date of the latest file.</li>
     * </ol>
     */
    private ArrayList<String> getDownloadURL(String datastore) {
        String username = properties.getProperty("userName");
        String repo_name = properties.getProperty("repoName");
        String remoteHostExtractFilesDir = properties.getProperty("remoteHostExtractFilesDir");
        String branch = properties.getProperty("branch");
        HttpHeaders headers = new HttpHeaders();
        headers.add("content-type", "application/json");

        String s = "https://api.github.com/repos/";
        String URL = (new StringBuilder()).append(s).append(username).append("/").append(repo_name).append("/contents/").append(remoteHostExtractFilesDir).append("/").append(datastore).append("?ref=").append(branch).toString();

        logger.info(URL);
        ResponseEntity<String> response = restTemplate.exchange(URL,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class);

        String json = response.getBody();
        Gson gson = new Gson();

        GitJson[] gitjsonlist = gson.fromJson(json, GitJson[].class);
        GitJson[] gitjsonarr = null;
        if (gitjsonlist[0].getName().equals(".gitignore")) {
            gitjsonarr = Arrays.copyOfRange(gitjsonlist, 1, gitjsonlist.length - 1);
        } else {
            gitjsonarr = Arrays.copyOfRange(gitjsonlist, 0, gitjsonlist.length - 1);
        }

        ArrayList<Date> dates = new ArrayList<>();
        Date date = null;

        for (GitJson element : gitjsonarr) {
            String datestr = element.getName().substring(0, 10);

            try {
                date = new SimpleDateFormat("MM-dd-yyyy").parse(datestr);
                dates.add(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
        Date maxDate = Collections.max(dates);
        Integer index = dates.indexOf(maxDate);
        String download_url = gitjsonlist[index + 1].getDownload_url();
        ArrayList<String> returnValues = new ArrayList<>();
        returnValues.add(download_url);
        returnValues.add(String.valueOf(maxDate));
        return returnValues;


    }

    /**
     * Downloads the data files for a given {@link ExtractDataStore}.
     * <p>
     * Uses {@link RestTemplate} to download the file from GitHub and stores it in the
     * temporary directory specified in {@link CovidUtil#EXTRACT_OUTPUT_DIR}.
     * Supports downloading both CSV and ZIP files.
     *
     * @param extractDataStore the datastore object containing metadata about the extraction.
     */
    private void downloadDataFiles(ExtractDataStore extractDataStore) {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));
        headers.add("content-type", "application/json");
        String dataStore = extractDataStore.getName();
        ArrayList<String> returnValues = getDownloadURL(dataStore);
        String download_url = returnValues.get(0);
        String fileName = returnValues.get(1);
        String extractDir = properties.getProperty(CovidUtil.EXTRACT_OUTPUT_DIR);

        File file = restTemplate.execute(download_url, HttpMethod.GET, null, clientHttpResponse -> {
            File ret = File.createTempFile(fileName, "csv", new File(extractDir));
            StreamUtils.copy(clientHttpResponse.getBody(), new FileOutputStream(ret));
            return ret;
        });

        List<String> files = new ArrayList<>();
        files.add(file.getAbsolutePath());
        setDataFiles(files);

    }

    /**
     * Sets the list of downloaded data files.
     *
     * @param dataFiles list of absolute paths to the downloaded data files.
     */

    private void setDataFiles(List<String> dataFiles) {
        this.dataFiles = dataFiles;
    }


    /**
     * Return Data from the source for the given datastore. Data expected to be returned as a Resultset.
     * containing a {@link ResultSet} for downstream processing.
     *
     * @param extractDataStore the datastore for which data needs to be queried.
     * @return a {@link ResultSet} containing the ResultSet and metadata.
     */
    @Override
    public ResultSet queryData(ExtractDataStore extractDataStore) {
        logger.info("QUERY DATA");
        downloadDataFiles((extractDataStore));
        return fileQueryData(extractDataStore);
    }

    /**
     * Processes the downloaded files and returns a ResultSet wrapped in
     * {@link ResultSet}.
     * <p>
     * Handles unzipping of any compressed files, validates data availability, and prepares the CSV
     * ResultSet.
     *
     * @param extractDataStore the datastore to extract data for.
     * @return a {@link ResultSet} wrapper with the ResultSet.
     */
    private ResultSet fileQueryData(ExtractDataStore extractDataStore) {
        if (dataFiles != null && !dataFiles.isEmpty()) {
            try {
                for (String file : dataFiles) {
                    if (file.endsWith("zip")) {
                        unzip(file, false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            logger.warn("No files or No data found to extract for the data store: " + extractDataStore.getName());
            extractDataStore.getTaskExecutionSummary().setStatus("MISSING_DATASTORE");
        }

        ResultSet rs = new CSVResultSet(dataFiles, extractDataStore, properties);
        return rs;
    }

    /**
     * Returns datastores available for Augmentation to the warehouse.
     *
     * @return an {@link Iterator} over {@link DataStoreInfo} objects representing each datastore.
     */
    @Override
    public Iterator<DataStoreInfo> extractDataStores() {
        String username = properties.getProperty("userName");
        String repo_name = properties.getProperty("repoName");
        String remoteHostExtractFilesDir = properties.getProperty("remoteHostExtractFilesDir");
        String branch = properties.getProperty("branch");
        HttpHeaders headers = new HttpHeaders();
        headers.add("content-type", "application/json");
        String s = "https://api.github.com/repos/";
        String URL = (new StringBuilder()).append(s).append(username).append("/").append(repo_name).append("/contents/").append(remoteHostExtractFilesDir).append("?ref=").append(branch).toString();
        ResponseEntity<String> response = restTemplate.exchange(URL,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class);
        String json = response.getBody();
        Gson gson = new Gson();
        GitJson[] gitjsonlist = gson.fromJson(json, GitJson[].class);
        ArrayList<GitJson> gitjsonarr = new ArrayList<>();
        for (GitJson gitJsonElement : gitjsonlist) {
            if (gitJsonElement.getType().equals("dir") && !blacklistDatastore.contains(gitJsonElement.getName())) {
                gitjsonarr.add(gitJsonElement);
            }
        }

        ArrayList<DataStoreInfo> dataStoreInfo = new ArrayList<>();
        for (GitJson element : gitjsonarr) {

            DataStoreInfo dataStoreInfoelement = new DataStoreInfo();
            dataStoreInfoelement.setName(element.getName());
            dataStoreInfoelement.setLabel(element.getName());
            dataStoreInfo.add(dataStoreInfoelement);
        }
        return dataStoreInfo.iterator();
    }

    /**
     * Returns a list of columns for a given datastore.
     * <p>
     * Downloads the CSV file for the datastore and reads the first line to determine column names.
     * Maps each column to an Oracle-supported data type and identifies primary key and last update
     * date columns.
     *
     * @param datastore the datastore to extract column metadata for.
     * @return a list of {@link DataStoreColumnMeta} representing the columns in the datastore.
     */
    private List<DataStoreColumnMeta> columnNames(String datastore) {

        ArrayList<String> downloadList = getDownloadURL(datastore);
        String download_url = downloadList.get(0);
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_OCTET_STREAM));
        headers.add("content-type", "application/json");
        String extractDir = properties.getProperty(CovidUtil.EXTRACT_OUTPUT_DIR);

        File file = restTemplate.execute(download_url, HttpMethod.GET, null, clientHttpResponse -> {
            File ret = File.createTempFile("temp", "csv", new File(extractDir));
            StreamUtils.copy(clientHttpResponse.getBody(), new FileOutputStream(ret));
            return ret;
        });
        String line = null;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            line = br.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<DataStoreColumnMeta> dataStoreColumnMetas = new ArrayList<>();
        if (!StringUtils.isBlank(line)) {
            List<String> columns = Arrays.asList(line.split(","));
            for (String column : columns) {

                DataStoreColumnMeta dataStoreMeta = new DataStoreColumnMeta();
                String dataType;
                if (columnDataTypeMap.containsKey(column)) {
                    dataType = columnDataTypeMap.get(column);
                } else {
                    dataType = ExtractorUtil.getStringDataType();
                }
                if (column.equals(dataStoreToPrimaryKeyColumnMap.get(datastore))) {
                    dataStoreMeta.setIsPrimaryKey(true);
                    dataStoreMeta.setKeySequence("1");
                }
                if (datastore.equals("csse_covid_19_daily_reports_us") && column.equals("Last_Update")) {
                    dataStoreMeta.setIsLastUpdateDate(true);
                }
                dataStoreMeta.setDataType(dataType);
                dataStoreMeta.setSize(String.valueOf(ExtractorUtil.getSize(dataType)));
                dataStoreMeta.setPrecision(String.valueOf(ExtractorUtil.getPrecision(dataType)));
                dataStoreMeta.setScale(String.valueOf(ExtractorUtil.getScale(dataType)));
                dataStoreMeta.setName(column);
                dataStoreColumnMetas.add(dataStoreMeta);
            }
        }
        return dataStoreColumnMetas;
    }


    /**
     * Returns list of columns available for all datastores supported for augmentation.
     * <p>
     * Combines the datastore information from {@link #extractDataStores()} with
     * {@link #columnNames(String)} to produce a full metadata description for each datastore.
     *
     * @return an {@link Iterator} over {@link DataStoreMeta} objects containing column metadata.
     */
    @Override
    public Iterator<DataStoreMeta> extractDataStoreColumns() {


        Iterator<DataStoreInfo> dataStoreInfoIterator = extractDataStores();
        ArrayList<DataStoreMeta> dataStoreMeta = new ArrayList<>();

        while (dataStoreInfoIterator.hasNext()) {
            DataStoreMeta dataStoreMetaEle = new DataStoreMeta();
            String dataStoreName = dataStoreInfoIterator.next().getName();
            List<DataStoreColumnMeta> columns = columnNames(dataStoreName);
            dataStoreMetaEle.setName(dataStoreName);
            dataStoreMetaEle.setColumns(columns);
            dataStoreMeta.add(dataStoreMetaEle);
        }


        return dataStoreMeta.iterator();
    }

    /**
     * Verifies the connection to the data source (GitHub repository).
     * <p>
     * Makes a simple REST call to the repository URL to confirm accessibility.
     * Updates {@link TaskExecutionSummary} in case of connection issues.
     *
     * @param extractorConfig      configuration for the extractor.
     * @param taskExecutionSummary summary object for task execution status.
     * @throws CustomExtractException if the source is unreachable or returns a non-OK status.
     */
    @Override
    public void verifyConnection(ExtractorConfig extractorConfig, TaskExecutionSummary taskExecutionSummary) {
        init(extractorConfig);
        String username = properties.getProperty("userName");
        String repo_name = properties.getProperty("repoName");
        HttpHeaders headers = new HttpHeaders();
        headers.add("content-type", "application/json");

        String s = "https://api.github.com/repos/";

        String URL = (new StringBuilder()).append(s).append(username).append("/").append(repo_name).toString();
        ResponseEntity<String> response = restTemplate.exchange(URL,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class);
        logger.info(response.getBody());
        if (!response.getStatusCode().equals(HttpStatus.OK)) {
            throw new CustomExtractException("Unable to connect to source. Response " + response.getStatusCode().value());
        }

    }

    /**
     * Provides the source version. Return a map of the source version and release.
     *
     * @return a {@link Map} containing keys "version" and "release" with their corresponding values.
     */
    @Override
    public Map<String, String> getVersion() {
        return new HashMap<>() {{
            put("version", "0.0");
            put("release", "0.0");
        }};

    }

    @Override
    public void close() throws Exception {

    }

    class GitJson {
        @JsonProperty("name")
        private String name;

        @JsonProperty("download_property")
        private String download_url;

        @JsonProperty("type")
        private String type;

        public String getName() {
            return name;
        }

        public String getDownload_url() {
            return download_url;
        }

        public String getType() {
            return type;
        }
    }

    /**
     * Utility method to unzip a file.
     * <p>
     * Extracts all entries from the ZIP archive to the output directory. Handles "__MACOSX"
     * system directories and prevents Zip Slip vulnerabilities. Removes the ZIP file after extraction.
     *
     * @param fileName          absolute path of the ZIP file.
     * @param isMetadataExtract true if extracting metadata only; false if extracting data files.
     * @throws Exception if an error occurs during extraction.
     */
    private void unzip(String fileName, boolean isMetadataExtract) throws Exception {
        BufferedOutputStream dest = null;
        BufferedInputStream is = null;
        ZipFile zipfile = null;
        FileOutputStream fos = null;
        try {
            ZipEntry entry = null;
            File file = new File(fileName);
            zipfile = new ZipFile(file);
            Enumeration e = zipfile.entries();
            String outputDirPath = properties.getProperty(CovidUtil.EXTRACT_OUTPUT_DIR);
            while (e.hasMoreElements()) {
                entry = (ZipEntry) e.nextElement();
                File filePath = CovidUtil.zipSlipProtect(entry, Paths.get(outputDirPath)).toFile();

                if (entry.getName().startsWith("__MACOSX")) {
                    continue;
                }

                is = new BufferedInputStream(zipfile.getInputStream(entry));
                int count;
                byte data[] = new byte[4096];
                logger.info("Unzip - {}", outputDirPath + File.separator + entry.getName());
                fos = new FileOutputStream(filePath);
                dest = new BufferedOutputStream(fos, 4096);
                while ((count = is.read(data, 0, 4096)) != -1) {
                    dest.write(data, 0, count);
                }
                dest.flush();
                dest.close();

                dataFiles.add(outputDirPath + File.separator + entry.getName());
            }
            //Delete the zip file.
            file.delete();

            if (!isMetadataExtract) {
                dataFiles.remove(fileName);
            }
        } finally {
            CovidUtil.free(dest);
            CovidUtil.free(fos);
            CovidUtil.free(is);
            CovidUtil.free(zipfile);
        }
    }

}
