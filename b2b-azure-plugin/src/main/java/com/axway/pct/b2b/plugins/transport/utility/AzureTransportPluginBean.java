package com.axway.pct.b2b.plugins.transport.utility;

public class AzureTransportPluginBean {
	private String name;
	private String key;
	private String container;
	private String autoCreateContainer;
	private String uploadMode;
	private String uploadDestination;
	private String pickupPath;
	private String patternType;
	private String pickupPattern;
	private String postProcess;
	private String userMetadata;
	private String appendMetadata;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}

	public String getAutoCreateContainer() {
		return autoCreateContainer;
	}

	public void setAutoCreateContainer(String autoCreateContainer) {
		this.autoCreateContainer = autoCreateContainer;
	}

	public String getUploadMode() {
		return uploadMode;
	}

	public void setUploadMode(String uploadMode) {
		this.uploadMode = uploadMode;
	}

	public String getUploadDestination() {
		return uploadDestination;
	}

	public void setUploadDestination(String uploadDestination) {
		this.uploadDestination = uploadDestination;
	}

	public String getPickupPath() {
		return pickupPath;
	}

	public void setPickupPath(String pickupPath) {
		this.pickupPath = pickupPath;
	}

	public String getPatternType() {
		return patternType;
	}

	public void setPatternType(String patternType) {
		this.patternType = patternType;
	}

	public String getPickupPattern() {
		return pickupPattern;
	}

	public void setPickupPattern(String pickupPattern) {
		this.pickupPattern = pickupPattern;
	}

	/**
	 * @return the postProcess
	 */
	public String getPostProcess() {
		return postProcess;
	}

	/**
	 * @param postProcess the postProcess to set
	 */
	public void setPostProcess(String postProcess) {
		this.postProcess = postProcess;
	}

	public String getUserMetadata() {
		return userMetadata;
	}

	public void setUserMetadata(String userMetadata) {
		this.userMetadata = userMetadata;
	}

	public void setAppendMetadata(String appendMetadata) {
		this.appendMetadata = appendMetadata;
		
	}

	public String getAppendMetadata() {
		return appendMetadata;
	}

}
