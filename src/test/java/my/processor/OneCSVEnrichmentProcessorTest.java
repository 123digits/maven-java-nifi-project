package my.processor;

import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;

public class OneCSVEnrichmentProcessorTest {

	@Test
	public void testGetEnrichmentColumns() {
		Assert.assertEquals(Arrays.asList("lat", "lon", "city", "country"),
				new OneCSVEnrichmentProcessor().getEnrichmentColumns());
	}
}
