package com.axway.pct.b2b.plugins.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.axway.pct.b2b.plugins.transport.utility.AzureTransportPluginBean;
import com.axway.pct.b2b.plugins.transport.utility.AzureTransportUtility;
import com.axway.pct.b2b.plugins.transport.utility.PluginConstants;
import com.cyclonecommerce.tradingengine.transport.FileNotFoundException;
import com.cyclonecommerce.tradingengine.transport.TransportInitializationException;
import com.cyclonecommerce.tradingengine.transport.TransportTestException;
import com.cyclonecommerce.tradingengine.transport.UnableToAuthenticateException;
import com.cyclonecommerce.tradingengine.transport.UnableToConnectException;
import com.cyclonecommerce.tradingengine.transport.UnableToConsumeException;
import com.cyclonecommerce.tradingengine.transport.UnableToDeleteException;
import com.cyclonecommerce.tradingengine.transport.UnableToDisconnectException;
import com.cyclonecommerce.tradingengine.transport.UnableToProduceException;
import com.cyclonecommerce.tradingengine.transport.pluggable.api.PluggableClient;
import com.cyclonecommerce.tradingengine.transport.pluggable.api.PluggableException;
import com.cyclonecommerce.tradingengine.transport.pluggable.api.PluggableMessage;
import com.cyclonecommerce.tradingengine.transport.pluggable.api.PluggableSettings;
import com.cyclonecommerce.util.VirtualData;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * @author cmanda
 *
 */
public class AzureBlobPluggableTransport implements PluggableClient {

	private static final Logger log = Logger.getLogger(AzureBlobPluggableTransport.class);
	private static final String LOGGER_KEY = "[Azure-Blob-Plugin]: ";

	// Consume & Produce plugin settings
	private static final String SETTING_NAME = "Name";
	private static final String SETTING_KEY = "Key";
	private static final String SETTING_CONTAINER = "Container";

	// Consume plugin settings
	private static final String SETTING_PICKUP_PATH = "Remote Path";
	private static final String SETTING_PICKUP_PATTERN = "Filter";
	private static final String SETTING_PATTERN_TYPE = "Type";
	private static final String SETTING_POST_PROCESS = "Post Process";

	// Produce plugin settings
	private static final String SETTING_AUTO_CREATE = "Auto Create";
	private static final String SETTING_DELIVERY_PATH = "Delivery Path";
	private static final String SETTING_DELIVERY_MODE = "Delivery Mode";
	private static final String SETTING_USER_METADATA = "User Metadata";
	private static final String SETTING_APPEND_AXWAY_METADATA = "Append Axway Metadata";

	// Setting to distinguish pickup and delivery mode
	private static final String SETTING_EXCHANGE_TYPE = "Exchange Type";

	private Map<String, String> constantProperties = null;

	private AzureTransportPluginBean config = null;
	private CloudStorageAccount mStorageAccount = null;
	private CloudBlobClient mBlobClient = null;
	private CloudBlobContainer mBlobContainer;

	private String _exchangeType = null;

	private static final String _PROGRAM_VERSION = "1.0-POC";
	private static final String _PROGRAM_NAME = "Azure-Blob";

	public AzureBlobPluggableTransport() {
		if (null == log.getLevel())
			log.setLevel(Level.DEBUG);

		log.info(LOGGER_KEY
				+ String.format("Executing PluggableTransport: %s version: %s", _PROGRAM_NAME, _PROGRAM_VERSION));

	}

	/**
	 * Initialize the Pluggable Client Instance
	 * 
	 * @param pluggableSettings
	 * @throws TransportInitializationException
	 */
	@Override
	public void init(PluggableSettings pluggableSettings) throws TransportInitializationException {
		log.debug(LOGGER_KEY + "-- Initializing Transfer Plugin --");

		try {
			// Initialize all the constant properties
			constantProperties = new HashMap<String, String>(pluggableSettings.getConstantSettings());
			if (constantProperties != null && !constantProperties.isEmpty()) {
				Iterator<String> iterator = constantProperties.keySet().iterator();
				while (iterator.hasNext()) {
					String key = iterator.next();
					log.debug(LOGGER_KEY + "Constant Property Setting [Key: " + key + ", Value: "
							+ constantProperties.get(key) + "]");
				}
			}
			_exchangeType = pluggableSettings.getConstantSetting(SETTING_EXCHANGE_TYPE);
			this.config = setUIConfiguration(pluggableSettings, _exchangeType);
		} catch (PluggableException e) {
			throw new TransportInitializationException("Error retrieving constant setting for pluggable transport", e);
		}

	}

	@SuppressWarnings("unused")
	private void setAzureBlobClient(CloudStorageAccount azureStorageAccount) {
		if (azureStorageAccount == null) {
			log.error(LOGGER_KEY + "Azure storage account not available. Unable to Initialize Storage Client.");
			return;
		}
		this.mBlobClient = azureStorageAccount.createCloudBlobClient();
	}

	private AzureTransportPluginBean setUIConfiguration(PluggableSettings pluggableSettings, String pluginType) {
		AzureTransportPluginBean bean = new AzureTransportPluginBean();
		if (null != pluggableSettings) {
			try {

				if (pluginType.equalsIgnoreCase(PluginConstants.EXCHANGE_PICKUP_NAME)) {

					bean.setContainer(pluggableSettings.getSetting(SETTING_CONTAINER));
					bean.setPickupPattern(pluggableSettings.getSetting(SETTING_PICKUP_PATTERN));
					bean.setPickupPath(pluggableSettings.getSetting(SETTING_PICKUP_PATH));
					bean.setPatternType(pluggableSettings.getSetting(SETTING_PATTERN_TYPE));
					bean.setKey(pluggableSettings.getSetting(SETTING_KEY));
					bean.setName(pluggableSettings.getSetting(SETTING_NAME));
					bean.setPostProcess(pluggableSettings.getSetting(SETTING_POST_PROCESS));
					log.debug(LOGGER_KEY + "Container		: " + bean.getContainer());
					log.debug(LOGGER_KEY + "Pickup Pattern	: " + bean.getPickupPattern());
					log.debug(LOGGER_KEY + "Pickup Path		: " + bean.getPickupPath());
					log.debug(LOGGER_KEY + "Pattern Type	: " + bean.getPatternType());
					log.debug(LOGGER_KEY + "Post Processing : " + bean.getPostProcess());
					log.debug(LOGGER_KEY + "Storage Account	: " + bean.getName());
				} else if(pluginType.equalsIgnoreCase(PluginConstants.EXCHANGE_DELIVERY_NAME)){

					bean.setAutoCreateContainer(pluggableSettings.getSetting(SETTING_AUTO_CREATE));
					bean.setContainer(pluggableSettings.getSetting(SETTING_CONTAINER));
					bean.setKey(pluggableSettings.getSetting(SETTING_KEY));
					bean.setName(pluggableSettings.getSetting(SETTING_NAME));
					bean.setUploadDestination(pluggableSettings.getSetting(SETTING_DELIVERY_PATH));
					bean.setUploadMode(pluggableSettings.getSetting(SETTING_DELIVERY_MODE));
					bean.setUserMetadata(pluggableSettings.getSetting(SETTING_USER_METADATA));
					bean.setAppendMetadata(pluggableSettings.getSetting(SETTING_APPEND_AXWAY_METADATA));

					log.debug(LOGGER_KEY + "Container				: " + bean.getContainer());
					log.debug(LOGGER_KEY + "Auto Create 			: " + bean.getAutoCreateContainer());
					log.debug(LOGGER_KEY + "Upload Destination		: " + bean.getUploadDestination());
					log.debug(LOGGER_KEY + "Upload Mode				: " + bean.getUploadMode());
					log.debug(LOGGER_KEY + "User Metadata			: " + bean.getUserMetadata());
					log.debug(LOGGER_KEY + "Add Interchange Metadata: " + bean.getAppendMetadata());
					log.debug(LOGGER_KEY + "Storage Account			: " + bean.getName());

				}

				log.info(LOGGER_KEY + "UI bean registered successfully for : " + pluginType);

			} catch (PluggableException e) {
				log.error(LOGGER_KEY + "Unable to set " + pluginType + " UI parameters: " + e.getMessage(), e);
			}
		}
		return bean;
	}

	@Override
	public void authenticate() throws UnableToAuthenticateException {
		log.debug(LOGGER_KEY + "-- Authentication -- " + _exchangeType);

		try {
			if (this.mStorageAccount == null) {
				this.mStorageAccount = CloudStorageAccount
						.parse(AzureTransportUtility.getConnectionString(this.config.getName(), this.config.getKey()));
				log.info(LOGGER_KEY + "Authentication being done again: "
						+ this.mStorageAccount.getBlobStorageUri().toString());
			} else {
				log.info(LOGGER_KEY + "Authentication already done to: "
						+ this.mStorageAccount.getBlobStorageUri().toString());
			}
		} catch (InvalidKeyException e) {
			log.error(LOGGER_KEY + e.getMessage(), e);
		} catch (URISyntaxException e) {
			log.error(LOGGER_KEY + e.getMessage(), e);
		}

	}

	@Override
	public void connect() throws UnableToConnectException {
		log.debug(LOGGER_KEY + "-- Connection -- " + _exchangeType);
		boolean autoCreateContainer = false;

		try {
			this.mStorageAccount = CloudStorageAccount
					.parse(AzureTransportUtility.getConnectionString(this.config.getName(), this.config.getKey()));
			this.mBlobClient = this.mStorageAccount.createCloudBlobClient();
			this.mBlobContainer = this.mBlobClient.getContainerReference(this.config.getContainer());
			autoCreateContainer = Boolean.parseBoolean(this.config.getAutoCreateContainer());
			log.debug(LOGGER_KEY + "Auto create container is set to: " + autoCreateContainer);
			log.debug(LOGGER_KEY + "Container exists?: " + this.mBlobContainer.getName());

			if (this.mBlobContainer.exists()) {
				log.info(LOGGER_KEY + "Container: " + this.config.getContainer() + " exists.");
			} else if (autoCreateContainer && _exchangeType.equalsIgnoreCase(PluginConstants.EXCHANGE_DELIVERY_NAME)) {
				log.info(LOGGER_KEY + "Container: " + this.config.getContainer() + " doesn't exist. Creating ...");
				log.info(LOGGER_KEY + "Auto-created container status: " + this.mBlobContainer.createIfNotExists());
			} else {
				log.error(LOGGER_KEY + "Container: " + this.config.getContainer()
						+ " doesn't exist. Auto-creating container is disabled");
				throw new TransportInitializationException("Container: " + this.config.getContainer()
						+ " doesn't exist. Auto-creating container is disabled");
			}
			log.info(LOGGER_KEY + "Connection successful to Container [" + this.mBlobContainer.getName() + "]");

		} catch (URISyntaxException e) {
			log.error(LOGGER_KEY + e.getMessage());
		} catch (StorageException e) {
			log.error(LOGGER_KEY + e.getMessage());
		} catch (TransportInitializationException e) {
			log.error(LOGGER_KEY + e.getMessage());
		} catch (InvalidKeyException e) {
			log.error(LOGGER_KEY + e.getMessage());
		}

	}

	@Override
	public PluggableMessage consume(PluggableMessage pluggableMessage, String nameFromList)
			throws UnableToConsumeException, FileNotFoundException {
		log.debug(LOGGER_KEY + "-- Preparing to Consume {PluggableMessage} from Blob Storage -- " + _exchangeType);
		CloudBlockBlob downloadBlob = null;
		String downloadPath = null;
		ByteArrayOutputStream targetStream = null;
		HashMap<String, String> blobMetadata = new HashMap<String, String>();
		VirtualData data = null;
		try {
			downloadPath = AzureTransportUtility.getSanitizedDownloadPath(nameFromList);
			downloadBlob = this.mBlobContainer.getBlockBlobReference(downloadPath);
			if (downloadBlob == null) {
				log.info(LOGGER_KEY + "Source blob doesn't exist at: " + downloadPath + ". Exiting ...");
				pluggableMessage = null;
				throw new UnableToConsumeException("Pluggable client cannot consume message(s)");
			} else {
				log.info(LOGGER_KEY + "Source Blob exists at: " + downloadPath);

				// set the metadata on the Interchange PluggableMessage
				blobMetadata = downloadBlob.getMetadata();
				log.info(LOGGER_KEY + "Source Blob Metadata has " + blobMetadata.size() + " entries");
				for (Iterator<String> iterator = blobMetadata.keySet().iterator(); iterator.hasNext();) {
					String key = (String) iterator.next();
					pluggableMessage.setMetadata(key, blobMetadata.get(key));
					log.info(LOGGER_KEY + "Source Blob Metadata [Key =" + key + ", Value=[" + blobMetadata.get(key)
							+ "]");
				}

				targetStream = new ByteArrayOutputStream();
				downloadBlob.download(targetStream);
				data = new VirtualData(targetStream.toByteArray());

				if (nameFromList.lastIndexOf(PluginConstants.AZURE_FOLDER_SEPARATOR) != -1) {
					nameFromList = nameFromList.substring(nameFromList.lastIndexOf(PluginConstants.AZURE_FOLDER_SEPARATOR) + 1);
				}

				pluggableMessage.setData(data);
				pluggableMessage.setFilename(nameFromList);

			}

		} catch (URISyntaxException e) {
			log.error(LOGGER_KEY + e.getMessage());
			throw new UnableToConsumeException("Unable to consume message." + e.getMessage(), e);
		} catch (StorageException e) {
			log.error(LOGGER_KEY + e.getMessage());
			throw new UnableToConsumeException("Unable to consume message." + e.getMessage(), e);
		}
		return pluggableMessage;
	}

	@Override
	public void delete(String blobName) throws UnableToDeleteException, FileNotFoundException {
		log.debug(LOGGER_KEY + "-- Delete --" + _exchangeType);
		CloudBlockBlob deleteBlob = null;
		try {
			deleteBlob = this.mBlobContainer.getBlockBlobReference(blobName);
			if (this.config.getPostProcess().equalsIgnoreCase(PluginConstants.POST_PROCESS_DELETE)) {
				boolean status = deleteBlob.deleteIfExists();
				log.info(LOGGER_KEY + "Post processing Status (delete): " + status);
			} else {
				log.info(LOGGER_KEY + "Post processing skipped: " + this.config.getPostProcess());
			}
		} catch (URISyntaxException | StorageException e) {
			log.error(LOGGER_KEY + e.getMessage());
			throw new UnableToDeleteException("Unable to Delete Source Blob [" + blobName + "]" + e.getMessage(), e);
		}
	}

	@Override
	public void disconnect() throws UnableToDisconnectException {
		log.debug(LOGGER_KEY + "-- Disconnect -- dereferencing plugin --" + _exchangeType);
		this.mStorageAccount = null;
	}

	@Override
	public String getUrl() throws PluggableException {
		StringBuffer url = new StringBuffer();
		log.debug(LOGGER_KEY + "-- getUrl -- " + _exchangeType);
		if (_exchangeType.equalsIgnoreCase(PluginConstants.EXCHANGE_PICKUP_NAME)) {
			url.append("azure-storage://").append(this.config.getContainer()).append("/")
					.append(this.config.getPickupPath()).append("/").append(this.config.getPickupPattern());
		} else if (_exchangeType.equalsIgnoreCase(PluginConstants.EXCHANGE_DELIVERY_NAME)) {
			url.append("azure-storage://").append(this.config.getContainer()).append("/")
					.append(this.config.getUploadDestination());
		}
		return url.toString();
	}

	@Override
	public boolean isPollable() {
		boolean isPollable = true;
		log.debug(LOGGER_KEY + "-- isPollable set to: " + isPollable + _exchangeType);
		return isPollable;
	}

	@Override
	public String[] list() throws UnableToConsumeException {
		log.debug(LOGGER_KEY + "-- List --" + _exchangeType);
		List<String> searchResults = new ArrayList<String>();
		CloudBlobContainer container;
		CloudBlobDirectory downloadDirectory;
		String containerName = this.config.getContainer();
		String pickupPath = this.config.getPickupPath();
		@SuppressWarnings("unused")
		String downloadObjectPrefix = null;
		String[] list = null;
		log.debug(LOGGER_KEY + "Listing Storage with Filtering criteria: " + pickupPath);
		if (pickupPath == null) {
			log.warn(LOGGER_KEY + "Pickup path defined as 'null'. Verify pluggable client [CONSUME] configuration");
		}
		pickupPath = AzureTransportUtility.getSanitizedDownloadPath(pickupPath);
		try {
			container = this.mBlobClient.getContainerReference(containerName);
			if (!container.exists()) {
				log.error(LOGGER_KEY + "Container [" + containerName + "] doesn't exist. Aborting download request");
			} else {
				downloadDirectory = container
						.getDirectoryReference(AzureTransportUtility.getDownloadDirectoryPath(pickupPath));
				downloadObjectPrefix = AzureTransportUtility.getTarget(pickupPath);
				searchResults = AzureTransportUtility.searchBlobItemList(this.config,
						getBlobItemList(downloadDirectory));
			}
		} catch (StorageException e) {
			log.error(LOGGER_KEY + e.getMessage());
			throw new UnableToConsumeException("Unable to initiate LIST request." + e.getMessage(), e);
		} catch (URISyntaxException e) {
			log.error(LOGGER_KEY + e.getMessage());
			throw new UnableToConsumeException("Unable to initiate LIST request." + e.getMessage(), e);
		} finally {
			log.info(LOGGER_KEY + "-- Items found matching pickup criteria: " + searchResults.size());
			list = new String[searchResults.size()];
			for (int i = 0; i < searchResults.size(); i++) {
				list[i] = searchResults.get(i);
				log.info(LOGGER_KEY + "-- Item[" + i + "]: " + list[i]);
			}

		}

		return list;
	}

	private ArrayList<ListBlobItem> getBlobItemList(CloudBlobDirectory blobDirectory) {
		ArrayList<ListBlobItem> blobList = null;
		try {
			blobList = Lists.newArrayList(blobDirectory.listBlobs());
			for (Iterator<ListBlobItem> iterator = blobList.iterator(); iterator.hasNext();) {
				ListBlobItem listBlobItem = (ListBlobItem) iterator.next();
				if (listBlobItem instanceof CloudBlob) {
					CloudBlob item = (CloudBlob) listBlobItem;
					log.debug(LOGGER_KEY + "--[Blob]		: " + item.getName());
				} else if (listBlobItem instanceof CloudBlobDirectory) {
					CloudBlobDirectory directory = (CloudBlobDirectory) listBlobItem;
					log.debug(LOGGER_KEY + "--[Directory] 	: " + directory.getUri().toString());
				}
			}
		} catch (StorageException e) {
			log.error(LOGGER_KEY + e.getMessage());
		} catch (URISyntaxException e) {
			log.error(LOGGER_KEY + e.getMessage());
		}

		return blobList;

	}

	@Override
	public PluggableMessage produce(PluggableMessage pluggableMessage, PluggableMessage returnMessage)
			throws UnableToProduceException {
		log.debug(LOGGER_KEY + "-- Produce --" + _exchangeType);

		String fileName = pluggableMessage.getFilename();
		String destinationDirectory = this.config.getUploadDestination();
		long fileSize = pluggableMessage.getData().length();
		String containerName = this.mBlobContainer.getName();
		boolean status = false;

		if (AzureTransportUtility.isFolderIncluded(fileName)) {
			throw new UnableToProduceException(
					"File name contains special characters used for folders in Azure. Aborting.");
		}

		try {
			fileName = AzureTransportUtility.constructUploadTargetFileName(fileName, destinationDirectory);
			boolean isRegularUploadMode = AzureTransportUtility.isRegularUploadMode(fileSize,
					this.config.getUploadMode());
			Map<String, String> customMetadata = AzureTransportUtility.getCustomMetadata(this.config);
			Map<String, String> pluggableMessageMetadata = pluggableMessage.getMetadata();
			// Sanitize & Update Interchange metadata to match Azure Blob Metadata
			if(this.config.getAppendMetadata().equalsIgnoreCase(PluginConstants.APPEND_METADATA_FLAG_YES)) {
				log.info(LOGGER_KEY + "Appending Interchange metadata to Blob Metadata");
				customMetadata.putAll(AzureTransportUtility.sanitizeMetadata(pluggableMessageMetadata));
			}
			log.info(LOGGER_KEY + "Azure Storage Upload to [" + containerName + "] under path [" + destinationDirectory
					+ "] for file [" + fileName + "] underway");
			if (isRegularUploadMode) {
				status = regularUpload(this.mBlobContainer, fileName, pluggableMessage, customMetadata, fileSize);
			} else {
				status = chunkUpload(this.mBlobContainer, fileName, pluggableMessage, customMetadata, fileSize);
			}
		} finally {
			String msg = LOGGER_KEY + "Azure storage upload completed [" + status + "]";
			log.info(msg);

			returnMessage.setData(new VirtualData(msg.toCharArray()));
		}
		return null;
	}

	private boolean chunkUpload(CloudBlobContainer container, String fileName, PluggableMessage pluggableMessage,
			Map<String, String> userMetadata, long fileSize) throws UnableToProduceException {
		boolean status = false;
		CloudBlockBlob chunkedBlob;
		int blockCount = (int) ((float) fileSize / (float) PluginConstants.BLOCK_SIZE) + 1;

		String containerName = container.getName();
		try {
			// container =
			// this.mBlobClient.getContainerReference(containerName);
			if (container.exists()) {
				log.info(LOGGER_KEY + "Container exists: " + containerName);
				// Create reference to the blob being uploaded -- filename
				chunkedBlob = container.getBlockBlobReference(fileName);

				log.info(LOGGER_KEY + "User metadata being applied ..." + userMetadata.size() + " key(s)");
				chunkedBlob.setMetadata((HashMap<String, String>) userMetadata);

				log.info(LOGGER_KEY + "Initiating blob upload with 'BlockList' mode");

				long bytesLeft = fileSize;
				int blockNumber = 0;
				long bytesRead = 0;

				log.info(LOGGER_KEY + "Block Size set to: " + PluginConstants.BLOCK_SIZE + ". Calculated blocks are: "
						+ blockCount);

				// Managed list of all block ids being uploaded
				List<BlockEntry> blockList = new ArrayList<BlockEntry>();
				InputStream input = FileUtils.openInputStream(pluggableMessage.getData().toFile());

				// Iterate & Upload individual blocks till the last block is
				// uploaded
				while (bytesLeft > 0) {

					blockNumber++;

					// how much to read (only last chunk may be smaller)
					// Source: http://www.redbaronofazure.com/?p=1
					long bytesToRead = 0;
					if (bytesLeft >= (long) PluginConstants.BLOCK_SIZE) {
						bytesToRead = PluginConstants.BLOCK_SIZE;
					} else {
						bytesToRead = bytesLeft;
					}

					// trace out progress
					float percentageDone = ((float) blockNumber / (float) blockCount) * (float) 100;

					// save block id in array (must be base64)
					String blockId = Base64.getEncoder()
							.encodeToString(String.format("BlockId%07d", blockNumber).getBytes(StandardCharsets.UTF_8));
					BlockEntry block = new BlockEntry(blockId);

					blockList.add(block);

					// upload block chunk to Azure Storage
					chunkedBlob.uploadBlock(blockId, input, (long) bytesToRead);
					log.info(LOGGER_KEY + " -- Block [Id: " + blockId + "]: " + String.format("%.0f%%", percentageDone)
							+ " uploaded. ");
					log.debug(LOGGER_KEY + " ---- Bytes Written in Block [Id:" + blockId + "]: " + bytesToRead
							+ ", [Upload Progress: Written( " + bytesRead + "), Left: (" + bytesLeft + ") ]");

					// increment/decrement counters
					bytesRead += bytesToRead;
					bytesLeft -= bytesToRead;
				}
				// Commit the block list to merge the blocks on Storage
				// NOTE: Microsoft removes orphan blocks after 7 days
				chunkedBlob.commitBlockList(blockList);
				input.close();

				log.info(LOGGER_KEY + "Block list with elements[" + blockList.size() + "] committed. Upload finished");
				if (chunkedBlob.exists())
					log.info(LOGGER_KEY + "Azure Blob 'multipart' upload finished successfully.");
			}
		} catch (URISyntaxException urie) {
			log.error(LOGGER_KEY + "Azure Resource URI constructed based on the containerName is invalid. "
					+ urie.getMessage());
			throw new UnableToProduceException(urie.getMessage(), urie);
		} catch (StorageException se) {
			log.error(LOGGER_KEY + "Azure Storage Service Error occurred. " + se.getMessage());
			throw new UnableToProduceException(se.getMessage(), se);
		} catch (IOException ioe) {
			log.error(LOGGER_KEY + "Azure Storage I/O exception occurred. " + ioe.getMessage());
			throw new UnableToProduceException(ioe.getMessage(), ioe);
		} finally {
			status = true;
		}
		return status;
	}

	private boolean regularUpload(CloudBlobContainer container, String fileName, PluggableMessage pluggableMessage,
			Map<String, String> customMetadata, long fileSize) throws UnableToProduceException {
		boolean status = false;
		CloudBlockBlob singleBlob;
		String containerName = container.getName();
		try {

			if (container.exists()) {
				log.info(LOGGER_KEY + "Container exists: " + containerName);
				singleBlob = container.getBlockBlobReference(fileName);

				log.info(LOGGER_KEY + "User metadata being applied ..." + customMetadata.size() + " key(s)");
				singleBlob.setMetadata((HashMap<String, String>) customMetadata);
				log.info(LOGGER_KEY + "Initiating blob upload with regular mode");

				singleBlob.upload(FileUtils.openInputStream(pluggableMessage.getData().toFile()), fileSize);
				if (singleBlob.exists())
					log.info(LOGGER_KEY + "Azure Blob upload finished successfully.");
			}
		} catch (URISyntaxException urie) {
			log.error(LOGGER_KEY + "Azure Resource URI constructed based on the containerName is invalid. "
					+ urie.getMessage());
			throw new UnableToProduceException(urie.getMessage(), urie);
		} catch (StorageException se) {
			log.error(LOGGER_KEY + "Azure Storage Service Error occurred. " + se.getMessage());
			throw new UnableToProduceException(se.getMessage(), se);
		} catch (IOException ioe) {
			log.error(LOGGER_KEY + "Azure Storage I/O exception occurred. " + ioe.getMessage());
			throw new UnableToProduceException(ioe.getMessage(), ioe);
		} finally {
			status = true;
		}
		return status;
	}

	@Override
	public String test() throws TransportTestException {
		StringBuffer testResult = new StringBuffer();
		testResult.append("There is nothing to test! :-)" + _exchangeType);
		return testResult.toString();

	}

}
