package com.axway.pct.b2b.plugins.transport.utility;

public class PluginConstants {

	public static final String AZURE_ACCOUNT_NAME = "AccountName";
	public static final String AZURE_ACCOUNT_KEY = "AccountKey";
	public static final String AZURE_UPLOAD_LOCATION = "UploadPath";
	public static final String AZURE_DOWNLOAD_LOCATION = "DownloadPath";
	public static final String AZURE_DOWNLOAD_PATTERN = "DownloadPattern";
	public static final String AZURE_CONTAINER_NAME = "Container";
	public static final String AZURE_AUTOCREATE_CONTAINER = "AutoCreateContainer";
	public static final String AZURE_FOLDER_SEPARATOR = "/";
	/**
	 * Azure Part size for each block is set to 10MB
	 */
	public static final long BLOCK_SIZE = (10 * 1024 * 1024);
	public static final String EXCHANGE_DELIVERY_NAME = "delivery";
	public static final String EXCHANGE_PICKUP_NAME = "pickup";
	public static final String POST_PROCESS_NONE = "none";
	public static final String POST_PROCESS_DELETE = "delete";
	public static final String APPEND_METADATA_FLAG_YES = "yes";
	public static final String APPEND_METADATA_FLAG_NO = "no";

}
