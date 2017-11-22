package com.axway.pct.b2b.plugins.transport.utility.pattern;

import java.util.ArrayList;

public class PatternTest {
	public static void main(String[] args) {
		ArrayList<String> result = new ArrayList<String>();
		String blobName = "test2/download.png";
		String downloadPattern = "*.png";
		String patternType = "glob";
				
		PatternKeyValidator validator = PatternKeyValidatorFactory.createPatternValidator(patternType);
		if(validator.isValid(blobName, downloadPattern)) {
			result.add(blobName);
			System.out.println(" --- Matched Item[downloadPattern="+downloadPattern+", patternType="+patternType+"]: " + blobName);
		} else {
			System.out.println(" --- No Matched Item[downloadPattern="+downloadPattern+", patternType="+patternType+"]: " + blobName);
		}	
	}

}
