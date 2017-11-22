package com.axway.pct.b2b.plugins.transport.utility.pattern;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

public class GlobValidator implements PatternKeyValidator {

	private static final String GLOB_KEYWORD = "glob:";

	@Override
	public boolean isValid(String input, String pattern) {
		PathMatcher matcher = FileSystems.getDefault().getPathMatcher(GLOB_KEYWORD + pattern);
		Path keyPath = Paths.get(input, new String[0]);

		return matcher.matches(keyPath);
	}

}
