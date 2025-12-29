package oracle.samples.extract.impl;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;
import oracle.apps.bi.custom.extractservice.exception.CustomExtractException;
import oracle.apps.bi.custom.extractservice.extract.ExtractDataStore;
import oracle.apps.bi.custom.extractservice.extract.util.ExtractorUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of a CSV Resultset that read data line by line from a CSV data file.
 */
public class CSVResultSet implements ResultSet {
    public static final String CSV_DATETIME_FORMAT = "CSV_DATETIME_FORMAT";
    public static final String CSV_DATE_FORMAT = "CSV_DATE_FORMAT";
    public static final String CSV_DELIMITER = "CSV_DELIMITER";

    protected BufferedReader reader;
    protected CSVReader csvReader;
    protected String[] currentLine;
    protected String[] fieldValues;
    protected String[] columnNames;
    protected List<String> availableColumns;
    protected String[] columnTypes;
    CSVResultSetMetadata csvResultSetMetadata;
    protected Map<String, Integer> columnIndexMap;
    List<String> files;
    int currentFile;
    ExtractDataStore extractDataStore;
    Properties properties;
    Character splitPattern;
    protected int rowCount;
    protected boolean invalidChar = false;
    protected StringBuilder invalidRows;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static final String DEFAULT_CSV_DELIMITER = ",";
    protected static final String DEFAULT_CSV_ENCLOSURE = "\"";
    public static final String DELIMITER = "DELIMITER";
    public static final String DATA_EXTRACTION_ERROR_MSG = "Error in parsing data for datastore";

    public CSVResultSet() {
    }

    public CSVResultSet(List<String> files, ExtractDataStore extractDataStore, Properties properties) {
        this.files = files;
        this.extractDataStore = extractDataStore;
        this.properties = properties;
        this.splitPattern = getCsvDelimiter();
        initializeCurrentFile();
    }

    protected Character getCsvDelimiter() {
        //if DELIMITER is set use the same
        String deLimiter;
        if (properties.getProperty(DELIMITER) != null) {
            deLimiter = properties.getProperty(DELIMITER);
            return deLimiter.charAt(0);
        }
        deLimiter = properties.getProperty(CSV_DELIMITER, DEFAULT_CSV_DELIMITER);
        return deLimiter.charAt(0);
    }

    protected void initializeCurrentFile() {
        InputStream inputStream = null;
        try {
            //Close any previous opened files.
            closeResources();

            //If previous file had invalid data, throw exception.
            if (invalidChar) {
                throw new CustomExtractException("The following rows " + StringUtils.substring(invalidRows.toString(), 0, 4000) + " has invalid data while processing the file");
            }

            if (columnNames == null) {
                availableColumns = new ArrayList<>();
                if (extractDataStore.getColumnOrder() != null && !extractDataStore.getColumnOrder().isEmpty()) {
                    columnNames = new String[extractDataStore.getColumnOrder().size()];
                    int index = 0;
                    for (String col : extractDataStore.getColumnOrder()) {
                        columnNames[index] = extractDataStore.nullSafeColumnAliasMap().get(col) != null ? extractDataStore.nullSafeColumnAliasMap().get(col) : col;
                        if (extractDataStore.getColumnToDataTypeMap().get(columnNames[index]) != null ||
                                (extractDataStore.getDataStore() != null)) {
                            availableColumns.add(columnNames[index]);
                        }
                        index++;
                    }
                }
            }

            String fileName = null;
            if (files != null && currentFile < files.size()) {
                rowCount = 0;

                fileName = files.get(currentFile++);
                logger.info("Processing file {}", fileName);
                //gzip
                if (fileName.endsWith(".gz")) {
                    inputStream = new GZIPInputStream(new FileInputStream(fileName));
                } else {
                    inputStream = new FileInputStream(fileName);
                }

                this.reader = new BufferedReader(new InputStreamReader(inputStream));

                CSVParser parser = new CSVParserBuilder()
                        .withSeparator(splitPattern).build();
                csvReader = new CSVReaderBuilder(this.reader).withCSVParser(parser).build();
                currentLine = csvReader.readNext();
                String[] headers = getHeaderValues();
                columnIndexMap = new LinkedCaseInsensitiveMap<>();
                int colIndex = 0;
                for (String val : headers) {
                    columnIndexMap.put(val, colIndex++);
                }

                if (columnNames == null) {
                    columnNames = headers;
                    if (columnNames != null) {
                        availableColumns = new ArrayList<>();
                        for (String col : columnNames) {
                            if (extractDataStore.getColumnToDataTypeMap().get(col) != null ||
                                    (extractDataStore.getDataStore() != null)) {
                                availableColumns.add(col);
                            }
                        }
                    }
                }
                if (currentFile > 1)
                    currentLine = csvReader.readNext();
            }

            if (columnNames == null) {
                throw new CustomExtractException("No data files or columns defined to fetch data");
            }

            if (columnTypes == null) {
                int index = 0;

                columnTypes = new String[availableColumns.size()];
                for (String d : availableColumns) {
                    String dataType = null;
                    if (extractDataStore.getColumnToDataTypeMap() != null) {
                        if (extractDataStore.getColumnToDataTypeMap().get(d.toUpperCase()) != null) {
                            dataType = extractDataStore.getColumnToDataTypeMap().get(d.toUpperCase());
                        } else {
                            dataType = extractDataStore.getColumnToDataTypeMap().get(d);
                        }
                    }

                    if (dataType != null)
                        columnTypes[index] = dataType;
                    else
                        columnTypes[index] = "NONE";
                    index++;
                }
                logger.info("columnTypes {}", Arrays.asList(columnTypes));
                logger.info("columnNames {}", Arrays.asList(columnNames));
                logger.info("Available columnNames {}", availableColumns);
            }
            String[] columns = availableColumns.stream().toArray(String[]::new);
            csvResultSetMetadata = new CSVResultSetMetadata(columns, columnTypes, extractDataStore);
            invalidChar = false;
            invalidRows = new StringBuilder();

        } catch (Exception e) {
            closeResources();
            CovidUtil.free(inputStream);
            logger.error("Error initializeCurrentFile", e);
            extractDataStore.setErrorFiles(Arrays.asList(files.get(currentFile-1)));
            throw new CustomExtractException(DATA_EXTRACTION_ERROR_MSG);
        }
    }

    protected void closeResources() {
        try {
            if (reader != null) {
                reader.close();
            }
            if (csvReader != null) {
                csvReader.close();
            }

        } catch (IOException e) {
            logger.error("CSVResultSet- Error while closing resources", e);
        }
    }

    private String[] getLineValues() {
        String[] values = getCurrentLineValues();
        rowCount++;
        if (values != null && values.length < columnIndexMap.keySet().size()) {
            logger.warn("Expecting " + columnIndexMap.keySet().size() + " columns in row " + rowCount + ", but found only " + values.length + " columns");
            invalidChar = true;
            if (invalidRows.length() > 0)
                invalidRows.append(",");
            invalidRows.append(rowCount);
            return null;
        }

        String[] data = new String[columnNames.length];
        if (values != null) {
            int index = 0;
            for (String col : availableColumns) {
                if (columnIndexMap.get(col) != null) {
                    int colIndex = columnIndexMap.get(col);
                    String val = values[colIndex];
                    //Remove surrounding quote characters.
                    if (val.length() >= 2 && val.startsWith(DEFAULT_CSV_ENCLOSURE) && val.endsWith(DEFAULT_CSV_ENCLOSURE)) {
                        val = val.substring(1, val.length() - 1);
                    }
                    data[index] = val;
                } else if (col.equalsIgnoreCase(ExtractorUtil.SYSTEM_GENERATED_SURROGATE_KEY)) {
                    data[index] = UUID.randomUUID().toString();
                } else {
                    // column is available in metadata, so not marked as missing col, but missing in the source file.
                    data[index] = null;
                }
                index++;
            }
        }
        return data;
    }

    private String[] getHeaderValues() throws IOException {
        String[] values = getCurrentHeaderValues();
        rowCount++;
        if (values != null) {
            int index = 0;
            for (String val : values) {
                //Remove surrounding quote characters.
                if (val.length() >= 2 && val.startsWith(DEFAULT_CSV_ENCLOSURE) && val.endsWith(DEFAULT_CSV_ENCLOSURE)) {
                    val = val.substring(1, val.length() - 1);
                }
                values[index] = val.trim();
                index++;
            }
        }
        return values;
    }

    public String[] getCurrentLineValues() {
        return currentLine;
    }


    public String[] getCurrentHeaderValues() throws IOException {
        return currentLine;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            if (reader == null || (currentLine = csvReader.readNext()) == null) {
                initializeCurrentFile();
            }
            if (currentLine != null) {
                fieldValues = getLineValues();
                if (fieldValues == null)
                    next();
            }
            return currentLine != null;
        } catch (IOException e) {
            logger.error("Unable to read line from file for Row " + rowCount, e);
            extractDataStore.setErrorFiles(Arrays.asList(files.get(currentFile-1)));
            throw new SQLException(DATA_EXTRACTION_ERROR_MSG);
        } catch (CsvValidationException e) {
            logger.error("Unable to parse line from file for Row " + rowCount, e);
            extractDataStore.setErrorFiles(Arrays.asList(files.get(currentFile-1)));
            throw new RuntimeException(DATA_EXTRACTION_ERROR_MSG);
        } catch (Exception e) {
            logger.error("Unable to read line from file for Row " + rowCount, e);
            extractDataStore.setErrorFiles(Arrays.asList(files.get(currentFile-1)));
            throw new SQLException(DATA_EXTRACTION_ERROR_MSG);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            closeResources();
        } catch (Exception e) {
            logger.error("CSVResultSet- Error while closing file", e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (fieldValues != null)
            return fieldValues[columnIndex - 1];
        return null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null) {
            Boolean b = Boolean.parseBoolean(s);
            if (b != null)
                return b;
        }
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        try {
            Byte b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Byte.valueOf(Byte.parseByte(str));
            return b;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        try {
            Short b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Short.valueOf(Short.parseShort(str));
            return b;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        try {
            Integer b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Integer.valueOf(Integer.parseInt(str));
            return b != null ? b : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        try {
            Long b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Long.valueOf(Long.parseLong(str));
            return b != null ? b : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        try {
            Float b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Float.valueOf(Float.parseFloat(str));
            return b != null ? b : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        try {
            Double b;
            String str = getString(columnIndex);
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Double.valueOf(Double.parseDouble(str));
            return b != null ? b : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            byte[] b;
            String str = getString(columnIndex);
            if (str == null)
                b = null;
            else
                b = str.getBytes();
            return b;
        } catch (RuntimeException e) {
            return null;
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        String fromDateFormat = properties.getProperty(CSV_DATE_FORMAT);
        if (fromDateFormat != null && str != null && !str.isEmpty()) {
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat(fromDateFormat);
                return new java.sql.Date(dateFormat.parse(str).getTime());
            } catch(IllegalArgumentException e){
                logger.error("Invalid pattern provided for CSV Date format. Review the CSV Date format in source connection configuration.", e);
                throw new CustomExtractException("Invalid pattern provided for CSV Date format. Review the CSV Date format in source connection configuration. Exception: " + e.getMessage());
            } catch (ParseException e) {
                logger.error("Unable to parse date: {}", str);
                return null;
            }
        }
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        String fromTimestampFormat = properties.getProperty(CSV_DATETIME_FORMAT);
        if (fromTimestampFormat != null && str != null && !str.isEmpty()) {
            try {
                SimpleDateFormat timestampFormat = new SimpleDateFormat(fromTimestampFormat);
                return new java.sql.Timestamp(timestampFormat.parse(str).getTime());
            } catch(IllegalArgumentException e){
                logger.error("Invalid pattern provided for CSV Timestamp format. Review the CSV Timestamp format in source connection configuration.", e);
                throw new CustomExtractException("Invalid pattern provided for CSV Timestamp format. Review the CSV Timestamp format in source connection configuration. Exception: " + e.getMessage());
            } catch (ParseException e) {
                logger.error("Unable to parse date: {}", str);
                return null;
            }
        }
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return csvResultSetMetadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (columnNames != null) {
            int index = 1;
            for (String column : columnNames) {
                if (column.equalsIgnoreCase(columnLabel))
                    return index;
                index++;
            }
        }
        return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    class CSVResultSetMetadata implements ResultSetMetaData {
        String[] columnNames;
        String[] columTypes;
        ExtractDataStore extractDataStore;
        private Map<String, Integer> typeNameToTypeCode = new HashMap<String, Integer>() {
            private static final long serialVersionUID = -8819579540085202365L;

            {
                put("VARCHAR", Integer.valueOf(Types.VARCHAR));
                put("VARCHAR2", Integer.valueOf(Types.VARCHAR));
                put("STRING", Integer.valueOf(Types.VARCHAR));
                put("NONE", Integer.valueOf(Types.VARCHAR));
                put("BOOLEAN", Integer.valueOf(Types.BOOLEAN));
                put("BYTE", Integer.valueOf(Types.TINYINT));
                put("SHORT", Integer.valueOf(Types.SMALLINT));
                put("NUMBER", Integer.valueOf(Types.NUMERIC));
                put("INTEGER", Integer.valueOf(Types.INTEGER));
                put("LONG", Integer.valueOf(Types.BIGINT));
                put("FLOAT", Integer.valueOf(Types.FLOAT));
                put("DOUBLE", Integer.valueOf(Types.DOUBLE));
                put("DATE", Integer.valueOf(Types.DATE));
                put("TIME", Integer.valueOf(Types.TIME));
                put("TIMESTAMP", Integer.valueOf(Types.TIMESTAMP));
            }
        };

        public CSVResultSetMetadata(String[] columnNames, String[] columnTypes, ExtractDataStore extractDataStore) {
            this.columnNames = columnNames;
            this.columTypes = columnTypes;
            this.extractDataStore = extractDataStore;
        }

        @Override
        public int getColumnCount() throws SQLException {
            return columnNames.length;
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isCaseSensitive(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isSearchable(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isCurrency(int column) throws SQLException {
            return false;
        }

        @Override
        public int isNullable(int column) throws SQLException {
            return ResultSetMetaData.columnNullableUnknown;
        }

        @Override
        public boolean isSigned(int column) throws SQLException {
            return false;
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException {
            return 0;
        }

        @Override
        public String getColumnLabel(int column) throws SQLException {
            return columnNames[column - 1];
        }

        @Override
        public String getColumnName(int column) throws SQLException {
            return columnNames[column - 1];
        }

        @Override
        public String getSchemaName(int column) throws SQLException {
            return null;
        }

        @Override
        public int getPrecision(int column) throws SQLException {
            return 0;
        }

        @Override
        public int getScale(int column) throws SQLException {
            return 0;
        }

        @Override
        public String getTableName(int column) throws SQLException {
            return extractDataStore.getName();
        }

        @Override
        public String getCatalogName(int column) throws SQLException {
            return null;
        }
        @Override
        public int  getColumnType(int column) throws SQLException {
            String columnTypeName = getColumnTypeName(column);
            Integer value = typeNameToTypeCode.get(columnTypeName);
            return value != null ? value.intValue() : 0;
        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
            return columnTypes[column - 1];
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException {
            return true;
        }

        @Override
        public boolean isWritable(int column) throws SQLException {
            return false;
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException {
            return false;
        }

        @Override
        public String getColumnClassName(int column) throws SQLException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}