package org.ppds.test;

import org.ppds.core.Compiler;
import org.ppds.core.QueryProcessor;

public class AQueryProcessorFactory {
	public static QueryProcessor createQueryProcessor() {
		return new Compiler();
	}
}
