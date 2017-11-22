package com.axway.pct.b2b.plugins.transport.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.axway.pct.b2b.plugins.transport.utility.pattern.PatternKeyValidator;
import com.axway.pct.b2b.plugins.transport.utility.pattern.PatternKeyValidatorFactory;
import com.axway.util.StringUtil;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class AzureTransportUtility {
	private static final Logger log = Logger.getLogger(AzureTransportUtility.class);
	private static final String LOGGER_KEY = "[AzureSDKUtility]: ";
	private static final String AUTO_MODE = "auto";
	private static final String MULTIPART_MODE = "multipart";

	public AzureTransportUtility() {
		if (null == log.getLevel())
			log.setLevel(Level.DEBUG);

		log.info(LOGGER_KEY + String.format("Pluggable Transport SDK Utility"));
	}

	public static String getConnectionString(String azureAccountName, String azureAccountKey) {
		String connectionString = String.format(
				"DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
				azureAccountName, azureAccountKey);
		log.debug(LOGGER_KEY + "ConnectionString: " + connectionString);
		return connectionString;
	}

	public static String getSanitizedDownloadPath(String downloadObjectKey) {
		if (downloadObjectKey == null) {
			log.info(LOGGER_KEY + "Download object key set to the root of the container. Null value specified");
			downloadObjectKey = StringUtils.EMPTY;
		}

		if (downloadObjectKey.equalsIgnoreCase(PluginConstants.AZURE_FOLDER_SEPARATOR)) {
			log.info(LOGGER_KEY + "Download object key set to the root of the container");
			downloadObjectKey = StringUtils.EMPTY;
		}

		if (downloadObjectKey.startsWith(PluginConstants.AZURE_FOLDER_SEPARATOR)) {
			log.info(LOGGER_KEY + "Removing leading forward-slashes from download object key");
			downloadObjectKey = downloadObjectKey.substring(1, downloadObjectKey.length());
		}

		log.debug(LOGGER_KEY + "Formatting the Input to match Azure specification - " + downloadObjectKey);
		return downloadObjectKey;
	}

	public static String getSanitizedUploadPath(String uploadDestination) {
		String result = uploadDestination;

		if (uploadDestination.isEmpty()) {
			return result;
		}

		if (uploadDestination.startsWith(PluginConstants.AZURE_FOLDER_SEPARATOR)) {
			result = uploadDestination.substring(1);
		}

		if (!uploadDestination.endsWith(PluginConstants.AZURE_FOLDER_SEPARATOR)) {
			result = new StringBuilder().append(result).append(PluginConstants.AZURE_FOLDER_SEPARATOR).toString();
		}
		log.debug(LOGGER_KEY + "Sanitized version of Destination directory: " + result);

		return result;
	}

	public static String getTarget(String blobName) {
		String target = StringUtils.EMPTY;
		if (blobName.equalsIgnoreCase(StringUtils.EMPTY))
			return StringUtils.EMPTY;

		target = getSanitizedDownloadPath(blobName).substring(blobName.lastIndexOf("/") + 1);

		log.info(LOGGER_KEY + "Download blob identified as: " + target);
		return target;
	}

	public static boolean isRegularUploadMode(long fileSizeInBytes, String uploadMode) {
		boolean result = true;

		if (uploadMode.equalsIgnoreCase(AUTO_MODE)) {
			result = fileSizeInBytes < (1024 * 1024 * 1024);

			log.debug(LOGGER_KEY + "Auto mode is enabled. File Size is lesser than 100MB? [" + result
					+ "]. Multipart mode: [" + !result + "]");
		} else if (uploadMode.equalsIgnoreCase(MULTIPART_MODE)) {
			int chunks = (int) (fileSizeInBytes / PluginConstants.BLOCK_SIZE);
			log.debug(LOGGER_KEY + "Multipart mode selected. Data will be sent in " + PluginConstants.BLOCK_SIZE
					+ " byte sized chunks. Total Chunks are: " + chunks);
			result = false;
		} else {
			log.debug(LOGGER_KEY + "Regular upload mode is selected.");
		}
		return result;
	}

	public static List<String> searchBlobItemList(AzureTransportPluginBean config,
			ArrayList<ListBlobItem> blobItemList) {
		ArrayList<String> result = new ArrayList<String>();
		String downloadPattern = config.getPickupPattern();
		String patternType = config.getPatternType();

		for (ListBlobItem item : blobItemList) {
			if (item instanceof CloudBlob) {
				CloudBlob blob = (CloudBlob) item;
				if (downloadPattern.isEmpty()) {
					result.add(blob.getName());
					log.debug(LOGGER_KEY + " --- Matched Item[downloadPattern=(none)]: " + blob.getName());
				} else {
					PatternKeyValidator validator = PatternKeyValidatorFactory.createPatternValidator(patternType);
					if (validator.isValid(getTarget(blob.getName()), downloadPattern)) {
						result.add(blob.getName());
						log.debug(LOGGER_KEY + " --- Matched Item[downloadPattern=" + downloadPattern + ", patternType="
								+ patternType + "]: " + blob.getName());
					} else {
						log.debug(LOGGER_KEY + " --- No Matched Item[downloadPattern=" + downloadPattern
								+ ", patternType=" + patternType + "]: " + blob.getName());
					}
				}
			}
		}
		log.info(LOGGER_KEY + "Matched entries in Listing: " + result.size());

		return result;
	}

	public static Map<String, String> getCustomMetadata(AzureTransportPluginBean configuration) {
		Map<String, String> keyValuePairs = parseMetadata(configuration.getUserMetadata());
		for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
			log.debug(LOGGER_KEY + "User Metadata [Key = " + entry.getKey() + ", Value: " + entry.getValue() + "]");
		}
		return keyValuePairs;
	}

	private static Map<String, String> parseMetadata(String metaData) {
		Map<String, String> entries = new HashMap<String, String>();
		if (StringUtil.isNullOrEmpty(metaData)) {
			return entries;
		}

		Pattern pattern = Pattern.compile("(.*?)=(.*)");
		Matcher matcher = pattern.matcher(metaData);
		while (matcher.find()) {
			String key = matcher.group(1).trim();
			String value = matcher.group(2).trim();
			entries.put(key, value);
		}

		return entries;
	}

	public static Map<String, String> sanitizeMetadata(Map<String, String> pluggableMessageMetadata) {
		Map<String, String> azureFormatMetadata = new HashMap<String, String>();
		for (Iterator<String> iterator = pluggableMessageMetadata.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			String value = pluggableMessageMetadata.get(key);
			if (null == value || value.isEmpty()) {
				value = "EMPTY";
				log.info(LOGGER_KEY + "[NOTE]: Replacing null/empty values with 'EMPTY' for [Key: " + key
						+ "]. Should be properly handled for production grade deployments.");
			}
			if (key.contains(".")) {
				key = key.replace(".", "_");
				log.info(LOGGER_KEY + "[NOTE]: Replacing invalid key values ('.' with '_') [Key: " + key
						+ "]. Should be properly handled for production grade deployments.");
			}
			azureFormatMetadata.put(key.toLowerCase(), value);
			log.info(LOGGER_KEY + "Interchange Metadata [Key: " + key.toLowerCase() + ", Value: " + pluggableMessageMetadata.get(key)
					+ "]");

		}
		return azureFormatMetadata;
	}

	public static String getDownloadDirectoryPath(String downloadObjectKey) {
		String downloadDirectory = StringUtils.EMPTY;
		if (downloadObjectKey.equalsIgnoreCase(StringUtils.EMPTY))
			return StringUtils.EMPTY;

		downloadDirectory = getSanitizedDownloadPath(downloadObjectKey).substring(0,
				downloadObjectKey.lastIndexOf("/"));

		log.info(LOGGER_KEY + "Download directory defined as: " + downloadDirectory);
		return downloadDirectory;
	}

	public static boolean isFolderIncluded(String objectName) {
		return objectName.contains("/");
	}

	public static String constructUploadTargetFileName(String originalFileName, String uploadDestinationPath) {
		String result = originalFileName;
		log.debug(LOGGER_KEY + "Original File Name: " + originalFileName);
		log.debug(LOGGER_KEY + "Destination Directory Path: " + uploadDestinationPath);
		String uploadDestination = getSanitizedUploadPath(uploadDestinationPath);

		result = new StringBuilder().append(uploadDestination).append(originalFileName).toString();
		log.info(LOGGER_KEY + "Azure Upload Target: " + result);
		return result;
	}

}
