/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.util;

import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_DELIMITER;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_ESCAPE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXTERNAL_SCAN_BUFFER_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_QUOTE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_END;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_START;
import static org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils.RESERVED_REGEX_CHARS;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.parsers.BooleanParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Response;

public class ExternalDataUtils {

    private static final Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = new EnumMap<>(ATypeTag.class);
    static {
        valueParserFactoryMap.put(ATypeTag.INTEGER, IntegerParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BIGINT, LongParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BOOLEAN, BooleanParserFactory.INSTANCE);
    }

    private ExternalDataUtils() {
    }

    public static int getOrDefaultBufferSize(Map<String, String> configuration) {
        String bufferSize = configuration.get(KEY_EXTERNAL_SCAN_BUFFER_SIZE);
        return bufferSize != null ? Integer.parseInt(bufferSize) : ExternalDataConstants.DEFAULT_BUFFER_SIZE;
    }

    // Get a delimiter from the given configuration
    public static char validateGetDelimiter(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER.charAt(0));
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char validateGetQuote(Map<String, String> configuration, char delimiter) throws HyracksDataException {
        char quote = validateCharOrDefault(configuration, KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE.charAt(0));
        validateDelimiterAndQuote(delimiter, quote);
        return quote;
    }

    public static char validateGetEscape(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_ESCAPE, ExternalDataConstants.ESCAPE);
    }

    public static char validateGetRecordStart(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_START, ExternalDataConstants.DEFAULT_RECORD_START);
    }

    public static char validateGetRecordEnd(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_END, ExternalDataConstants.DEFAULT_RECORD_END);
    }

    public static void validateDataParserParameters(Map<String, String> configuration) throws AsterixException {
        String parser = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (parser == null) {
            String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            if (parserFactory == null) {
                throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED,
                        ExternalDataConstants.KEY_FORMAT + " or " + ExternalDataConstants.KEY_PARSER_FACTORY);
            }
        }
    }

    public static void validateDataSourceParameters(Map<String, String> configuration) throws AsterixException {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if (reader == null) {
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_READER);
        }
    }

    public static DataSourceType getDataSourceType(Map<String, String> configuration) {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if ((reader != null) && reader.equals(ExternalDataConstants.READER_STREAM)) {
            return DataSourceType.STREAM;
        } else {
            return DataSourceType.RECORDS;
        }
    }

    public static boolean isExternal(String aString) {
        return ((aString != null) && aString.contains(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR)
                && (aString.trim().length() > 1));
    }

    public static ClassLoader getClassLoader(ILibraryManager libraryManager, String dataverse, String library) {
        return libraryManager.getLibraryClassLoader(dataverse, library);
    }

    public static String getLibraryName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
    }

    public static String getExternalClassName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[1];
    }

    public static IInputStreamFactory createExternalInputStreamFactory(ILibraryManager libraryManager, String dataverse,
            String stream) throws HyracksDataException {
        try {
            String libraryName = getLibraryName(stream);
            String className = getExternalClassName(stream);
            ClassLoader classLoader = getClassLoader(libraryManager, dataverse, libraryName);
            return ((IInputStreamFactory) (classLoader.loadClass(className).newInstance()));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY, e);
        }
    }

    public static String getDataverse(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_DATAVERSE);
    }

    public static String getParserFactory(Map<String, String> configuration) {
        String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER);
        if (parserFactory != null) {
            return parserFactory;
        }
        parserFactory = configuration.get(ExternalDataConstants.KEY_FORMAT);
        return parserFactory != null ? parserFactory : configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
    }

    public static IValueParserFactory[] getValueParserFactories(ARecordType recordType) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                AUnionType unionType = (AUnionType) recordType.getFieldTypes()[i];
                if (!unionType.isUnknownableType()) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionType.getActualType().getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            fieldParserFactories[i] = getParserFactory(tag);
        }
        return fieldParserFactories;
    }

    public static IValueParserFactory getParserFactory(ATypeTag tag) {
        IValueParserFactory vpf = valueParserFactoryMap.get(tag);
        if (vpf == null) {
            throw new NotImplementedException("No value parser factory for fields of type " + tag);
        }
        return vpf;
    }

    public static boolean hasHeader(Map<String, String> configuration) {
        return isTrue(configuration, ExternalDataConstants.KEY_HEADER);
    }

    public static boolean isTrue(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        return value == null ? false : Boolean.valueOf(value);
    }

    public static IRecordReaderFactory<?> createExternalRecordReaderFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        String readerFactory = configuration.get(ExternalDataConstants.KEY_READER_FACTORY);
        if (readerFactory == null) {
            throw new AsterixException("to use " + ExternalDataConstants.EXTERNAL + " reader, the parameter "
                    + ExternalDataConstants.KEY_READER_FACTORY + " must be specified.");
        }
        String[] libraryAndFactory = readerFactory.split(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR);
        if (libraryAndFactory.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        String[] dataverseAndLibrary = libraryAndFactory[0].split(".");
        if (dataverseAndLibrary.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }

        ClassLoader classLoader = libraryManager.getLibraryClassLoader(dataverseAndLibrary[0], dataverseAndLibrary[1]);
        try {
            return (IRecordReaderFactory<?>) classLoader.loadClass(libraryAndFactory[1]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create record reader factory", e);
        }
    }

    public static IDataParserFactory createExternalParserFactory(ILibraryManager libraryManager, String dataverse,
            String parserFactoryName) throws AsterixException {
        try {
            String library = parserFactoryName.substring(0,
                    parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
            ClassLoader classLoader = libraryManager.getLibraryClassLoader(dataverse, library);
            return (IDataParserFactory) classLoader
                    .loadClass(parserFactoryName
                            .substring(parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) + 1))
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create an external parser factory", e);
        }
    }

    public static boolean isFeed(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            return false;
        } else {
            return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_FEED));
        }
    }

    public static void prepareFeed(Map<String, String> configuration, String dataverseName, String feedName) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            configuration.put(ExternalDataConstants.KEY_IS_FEED, ExternalDataConstants.TRUE);
        }
        configuration.put(ExternalDataConstants.KEY_DATAVERSE, dataverseName);
        configuration.put(ExternalDataConstants.KEY_FEED_NAME, feedName);
    }

    public static boolean keepDataSourceOpen(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_WAIT_FOR_DATA)) {
            return true;
        }
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_WAIT_FOR_DATA));
    }

    public static String getFeedName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_FEED_NAME);
    }

    public static boolean isRecordWithMeta(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME);
    }

    public static void setRecordWithMeta(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.FORMAT_RECORD_WITH_METADATA, booleanString);
    }

    public static boolean isChangeFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_CHANGE_FEED));
    }

    public static boolean isInsertFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_INSERT_FEED));
    }

    public static int getNumberOfKeys(Map<String, String> configuration) throws AsterixException {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        if (keyIndexes == null) {
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_KEY_INDEXES);
        }
        return keyIndexes.split(",").length;
    }

    public static void setNumberOfKeys(Map<String, String> configuration, int value) {
        configuration.put(ExternalDataConstants.KEY_KEY_SIZE, String.valueOf(value));
    }

    public static void setChangeFeed(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.KEY_IS_CHANGE_FEED, booleanString);
    }

    public static int[] getPKIndexes(Map<String, String> configuration) {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        String[] stringIndexes = keyIndexes.split(",");
        int[] intIndexes = new int[stringIndexes.length];
        for (int i = 0; i < stringIndexes.length; i++) {
            intIndexes[i] = Integer.parseInt(stringIndexes[i]);
        }
        return intIndexes;
    }

    public static int[] getPKSourceIndicators(Map<String, String> configuration) {
        String keyIndicators = configuration.get(ExternalDataConstants.KEY_KEY_INDICATORS);
        String[] stringIndicators = keyIndicators.split(",");
        int[] intIndicators = new int[stringIndicators.length];
        for (int i = 0; i < stringIndicators.length; i++) {
            intIndicators[i] = Integer.parseInt(stringIndicators[i]);
        }
        return intIndicators;
    }

    /**
     * Fills the configuration of the external dataset and its adapter with default values if not provided by user.
     *
     * @param configuration external data configuration
     */
    public static void defaultConfiguration(Map<String, String> configuration) {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format != null) {
            // default quote, escape character for quote and fields delimiter for csv and tsv format
            if (format.equals(ExternalDataConstants.FORMAT_CSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.DEFAULT_QUOTE);
            } else if (format.equals(ExternalDataConstants.FORMAT_TSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.TAB_STR);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.NULL_STR);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.NULL_STR);
            }
        }
    }

    /**
     * Prepares the configuration of the external data and its adapter by filling the information required by
     * adapters and parsers.
     *
     * @param adapterName adapter name
     * @param configuration external data configuration
     */
    public static void prepare(String adapterName, Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_READER)) {
            configuration.put(ExternalDataConstants.KEY_READER, adapterName);
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_PARSER)
                && configuration.containsKey(ExternalDataConstants.KEY_FORMAT)) {
            configuration.put(ExternalDataConstants.KEY_PARSER, configuration.get(ExternalDataConstants.KEY_FORMAT));
        }
    }

    /**
     * Normalizes the values of certain parameters of the adapter configuration. This should happen before persisting
     * the metadata (e.g. when creating external datasets or feeds) and when creating an adapter factory.
     *
     * @param configuration external data configuration
     */
    public static void normalize(Map<String, String> configuration) {
        // normalize the "format" parameter
        String paramValue = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (paramValue != null) {
            String lowerCaseFormat = paramValue.toLowerCase().trim();
            if (ExternalDataConstants.ALL_FORMATS.contains(lowerCaseFormat)) {
                configuration.put(ExternalDataConstants.KEY_FORMAT, lowerCaseFormat);
            }
        }
        // normalize "header" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_HEADER);
        // normalize "redact-warnings" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_REDACT_WARNINGS);
    }

    /**
     * Validates the parameter values of the adapter configuration. This should happen after normalizing the values.
     *
     * @param configuration external data configuration
     * @throws HyracksDataException HyracksDataException
     */
    public static void validate(Map<String, String> configuration) throws HyracksDataException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        String header = configuration.get(ExternalDataConstants.KEY_HEADER);
        if (format != null && isHeaderRequiredFor(format) && header == null) {
            throw new RuntimeDataException(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_HEADER);
        }
        if (header != null && !isBoolean(header)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_HEADER, header);
        }
        char delimiter = validateGetDelimiter(configuration);
        validateGetQuote(configuration, delimiter);
        validateGetEscape(configuration);
        String value = configuration.get(ExternalDataConstants.KEY_REDACT_WARNINGS);
        if (value != null && !isBoolean(value)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_REDACT_WARNINGS,
                    value);
        }
    }

    private static boolean isHeaderRequiredFor(String format) {
        return format.equals(ExternalDataConstants.FORMAT_CSV) || format.equals(ExternalDataConstants.FORMAT_TSV);
    }

    private static boolean isBoolean(String value) {
        return value.equals(ExternalDataConstants.TRUE) || value.equals(ExternalDataConstants.FALSE);
    }

    private static void validateDelimiterAndQuote(char delimiter, char quote) throws RuntimeDataException {
        if (quote == delimiter) {
            throw new RuntimeDataException(ErrorCode.QUOTE_DELIMITER_MISMATCH, quote, delimiter);
        }
    }

    private static char validateCharOrDefault(Map<String, String> configuration, String key, char defaultValue)
            throws HyracksDataException {
        String value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        validateChar(value, key);
        return value.charAt(0);
    }

    public static void validateChar(String parameterValue, String parameterName) throws RuntimeDataException {
        if (parameterValue.length() != 1) {
            throw new RuntimeDataException(ErrorCode.INVALID_CHAR_LENGTH, parameterValue, parameterName);
        }
    }

    private static void putToLowerIfExists(Map<String, String> configuration, String key) {
        String paramValue = configuration.get(key);
        if (paramValue != null) {
            configuration.put(key, paramValue.toLowerCase().trim());
        }
    }

    /**
     * Validates adapter specific external dataset properties. Specific properties for different adapters should be
     * validated here
     *
     * @param configuration properties
     */
    public static void validateAdapterSpecificProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector) throws CompilationException {
        String type = configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE);

        switch (type) {
            case ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3:
                AwsS3.validateProperties(configuration, srcLoc, collector);
                break;
            default:
                // Nothing needs to be done
                break;
        }
    }

    /**
     * Regex matches all the provided patterns against the provided path
     *
     * @param path path to check against
     *
     * @return {@code true} if all patterns match, {@code false} otherwise
     */
    public static boolean matchPatterns(List<Matcher> matchers, String path) {
        for (Matcher matcher : matchers) {
            if (matcher.reset(path).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the wildcard to proper regex
     *
     * @param pattern wildcard pattern to convert
     *
     * @return regex expression
     */
    public static String patternToRegex(String pattern) {
        int charPosition = 0;
        int patternLength = pattern.length();
        StringBuilder stuffBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();

        while (charPosition < patternLength) {
            char c = pattern.charAt(charPosition);
            charPosition++;

            switch (c) {
                case '*':
                    result.append(".*");
                    break;
                case '?':
                    result.append(".");
                    break;
                case '[':
                    int closingBracketPosition = charPosition;
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == '!') {
                        closingBracketPosition++;
                    }

                    // 2 cases can happen here:
                    // 1- Empty character class [] which is invalid for java, so treat ] as literal and find another
                    // closing bracket, if no closing bracket is found, the whole thing is a literal
                    // 2- Negated empty class [!] converted to [^] which is invalid for java, so treat ] as literal and
                    // find another closing bracket, if no closing bracket is found, the whole thing is a literal
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == ']') {
                        closingBracketPosition++;
                    }

                    // No [] and [!] cases, search for the closing bracket
                    while (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) != ']') {
                        closingBracketPosition++;
                    }

                    // No closing bracket found (or [] or [!]), escape the opening bracket, treat it as literals
                    if (closingBracketPosition >= patternLength) {
                        result.append("\\[");
                    } else {
                        // Found closing bracket, get the stuff in between the found the character class ("[" and "]")
                        String stuff = pattern.substring(charPosition, closingBracketPosition);

                        stuffBuilder.setLength(0);
                        int stuffCharPos = 0;

                        // If first character in the character class is "!" then convert it to "^"
                        if (stuff.charAt(0) == '!') {
                            stuffBuilder.append('^');
                            stuffCharPos++; // ignore first character when escaping metacharacters next step
                        }

                        for (; stuffCharPos < stuff.length(); stuffCharPos++) {
                            char stuffChar = stuff.charAt(stuffCharPos);
                            if (stuffChar != '-' && Arrays.binarySearch(RESERVED_REGEX_CHARS, stuffChar) >= 0) {
                                stuffBuilder.append("\\");
                            }
                            stuffBuilder.append(stuffChar);
                        }

                        String stuffEscaped = stuffBuilder.toString();

                        // Escape the set operations
                        stuffEscaped = stuffEscaped.replace("&&", "\\&\\&").replace("~~", "\\~\\~")
                                .replace("||", "\\|\\|").replace("--", "\\-\\-");

                        result.append("[").append(stuffEscaped).append("]");
                        charPosition = closingBracketPosition + 1;
                    }
                    break;
                default:
                    if (Arrays.binarySearch(RESERVED_REGEX_CHARS, c) >= 0) {
                        result.append("\\");
                    }
                    result.append(c);
                    break;
            }
        }

        return result.toString();
    }

    /**
     * Adjusts the prefix (if needed) and returns it
     *
     * @param configuration configuration
     */
    public static String getPrefix(Map<String, String> configuration) {
        String definition = configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
        if (definition != null && !definition.isEmpty()) {
            return definition + (!definition.endsWith("/") ? "/" : "");
        }
        return "";
    }

    public static class AwsS3 {
        private AwsS3() {
            throw new AssertionError("do not instantiate");
        }

        /**
         * Builds the S3 client using the provided configuration
         *
         * @param configuration properties
         * @return S3 client
         * @throws CompilationException CompilationException
         */
        public static S3Client buildAwsS3Client(Map<String, String> configuration) throws CompilationException {
            // TODO(Hussain): Need to ensure that all required parameters are present in a previous step
            String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
            String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
            String regionId = configuration.get(ExternalDataConstants.AwsS3.REGION_FIELD_NAME);
            String serviceEndpoint = configuration.get(ExternalDataConstants.AwsS3.SERVICE_END_POINT_FIELD_NAME);

            S3ClientBuilder builder = S3Client.builder();

            // Credentials
            AwsCredentialsProvider credentialsProvider;

            // No auth required
            if (accessKeyId == null) {
                credentialsProvider = AnonymousCredentialsProvider.create();
            } else {
                credentialsProvider =
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
            }

            builder.credentialsProvider(credentialsProvider);
            builder.region(Region.of(regionId));

            // Validate the service endpoint if present
            if (serviceEndpoint != null) {
                try {
                    URI uri = new URI(serviceEndpoint);
                    try {
                        builder.endpointOverride(uri);
                    } catch (NullPointerException ex) {
                        throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
                    }
                } catch (URISyntaxException ex) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR,
                            String.format("Invalid service endpoint %s", serviceEndpoint));
                }
            }

            return builder.build();
        }

        /**
         * Validate external dataset properties
         *
         * @param configuration properties
         * @throws CompilationException Compilation exception
         */
        public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
                IWarningCollector collector) throws CompilationException {

            // check if the format property is present
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
            }

            // Both parameters should be passed, or neither should be passed (for anonymous/no auth)
            String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
            String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
            if (accessKeyId == null || secretAccessKey == null) {
                // If one is passed, the other is required
                if (accessKeyId != null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                            ACCESS_KEY_ID_FIELD_NAME);
                } else if (secretAccessKey != null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                            SECRET_ACCESS_KEY_FIELD_NAME);
                }
            }

            validateIncludeExclude(configuration);

            // Check if the bucket is present
            S3Client s3Client = buildAwsS3Client(configuration);;
            S3Response response;
            boolean useOldApi = false;
            String container = configuration.get(ExternalDataConstants.AwsS3.CONTAINER_NAME_FIELD_NAME);
            String prefix = getPrefix(configuration);

            try {
                response = isBucketEmpty(s3Client, container, prefix, false);
            } catch (S3Exception ex) {
                // Method not implemented, try falling back to old API
                try {
                    // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                    if (ex.awsErrorDetails().errorCode().equals("NotImplemented")) {
                        useOldApi = true;
                        response = isBucketEmpty(s3Client, container, prefix, true);
                    } else {
                        throw ex;
                    }
                } catch (SdkException ex2) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2.getMessage());
                }
            } catch (SdkException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
            } finally {
                if (s3Client != null) {
                    CleanupUtils.close(s3Client, null);
                }
            }

            boolean isEmpty = useOldApi ? ((ListObjectsResponse) response).contents().isEmpty()
                    : ((ListObjectsV2Response) response).contents().isEmpty();
            if (isEmpty && collector.shouldWarn()) {
                Warning warning =
                        WarningUtil.forAsterix(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }

            // Returns 200 only in case the bucket exists, otherwise, throws an exception. However, to
            // ensure coverage, check if the result is successful as well and not only catch exceptions
            if (!response.sdkHttpResponse().isSuccessful()) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, container);
            }
        }

        /**
         * Checks for a single object in the specified bucket to determine if the bucket is empty or not.
         *
         * @param s3Client s3 client
         * @param container the container name
         * @param prefix Prefix to be used
         * @param useOldApi flag whether to use the old API or not
         *
         * @return returns the S3 response
         */
        private static S3Response isBucketEmpty(S3Client s3Client, String container, String prefix, boolean useOldApi) {
            S3Response response;
            if (useOldApi) {
                ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder();
                listObjectsBuilder.prefix(prefix);
                response = s3Client.listObjects(listObjectsBuilder.bucket(container).maxKeys(1).build());
            } else {
                ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder();
                listObjectsBuilder.prefix(prefix);
                response = s3Client.listObjectsV2(listObjectsBuilder.bucket(container).maxKeys(1).build());
            }
            return response;
        }

        /**
         * @param configuration
         * @throws CompilationException
         */
        public static void validateIncludeExclude(Map<String, String> configuration) throws CompilationException {
            // Ensure that include and exclude are not provided at the same time + ensure valid format or property
            List<Map.Entry<String, String>> includes = new ArrayList<>();
            List<Map.Entry<String, String>> excludes = new ArrayList<>();

            // Accepted formats are include, include#1, include#2, ... etc, same for excludes
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                String key = entry.getKey();

                if (key.equals(ExternalDataConstants.KEY_INCLUDE)) {
                    includes.add(entry);
                } else if (key.equals(ExternalDataConstants.KEY_EXCLUDE)) {
                    excludes.add(entry);
                } else if (key.startsWith(ExternalDataConstants.KEY_INCLUDE)
                        || key.startsWith(ExternalDataConstants.KEY_EXCLUDE)) {

                    // Split by the "#", length should be 2, left should be include/exclude, right should be integer
                    String[] splits = key.split("#");

                    if (key.startsWith(ExternalDataConstants.KEY_INCLUDE) && splits.length == 2
                            && splits[0].equals(ExternalDataConstants.KEY_INCLUDE)
                            && NumberUtils.isIntegerNumericString(splits[1])) {
                        includes.add(entry);
                    } else if (key.startsWith(ExternalDataConstants.KEY_EXCLUDE) && splits.length == 2
                            && splits[0].equals(ExternalDataConstants.KEY_EXCLUDE)
                            && NumberUtils.isIntegerNumericString(splits[1])) {
                        excludes.add(entry);
                    } else {
                        throw new CompilationException(ErrorCode.INVALID_PROPERTY_FORMAT, key);
                    }
                }
            }

            // TODO: Should include/exclude be a common check or S3 specific?
            // Ensure either include or exclude are provided, but not both of them
            if (!includes.isEmpty() && !excludes.isEmpty()) {
                throw new CompilationException(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME,
                        ExternalDataConstants.KEY_INCLUDE, ExternalDataConstants.KEY_EXCLUDE);
            }
        }
    }
}
