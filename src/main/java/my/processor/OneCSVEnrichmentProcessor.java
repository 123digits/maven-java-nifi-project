package my.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.nifi.ssl.SSLContextService;

import my.util.AbstractCSVEnrichmentProcessor;

/**
 * Property set to "src_ip,dst_ip"<br>
 * When finding "src_ip" or "dst_ip" fields, it will be enriched with latitude,
 * longitude, city, and country.<br>
 * This class's getEnrichmentFields returns ["lat","lon","city","country"]<br>
 * For "src_ip", adding "src_ip_lat" "src_ip_lon" "src_ip_city"
 * "src_ip_country"<br>
 * For "dst_ip", adding "dst_ip_lat" "dst_ip_lon" "dst_ip_city"
 * "dst_ip_country"<br>
 */
public class OneCSVEnrichmentProcessor extends AbstractCSVEnrichmentProcessor {

	public List<String> getEnrichmentColumns() {
		return Arrays.asList("lat", "lon", "city", "country");
	}

	public byte[] getCsvHeaders(String line, String enrichmentHeaders) {
		return new byte[0];
	}

	public byte[] getCsvRow(String line, SSLContextService sslService) throws IOException {
		CSVParser csvParser = new CSVParser(null, CSVFormat.DEFAULT);
		return new byte[0];
	}

}
