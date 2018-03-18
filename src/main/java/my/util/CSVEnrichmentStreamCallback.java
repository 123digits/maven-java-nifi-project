package my.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;

import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.ssl.SSLContextService;
import org.bouncycastle.util.Arrays;

public class CSVEnrichmentStreamCallback implements StreamCallback {

	private String enrichmentHeaders;
	private SSLContextService sslService;
	private List<String> enrichmentFields;
	private int[] enrichmentColumns;

	public CSVEnrichmentStreamCallback(String enrichmentHeaders, SSLContextService sslService, List<String> enrichmentFields) {
		this.enrichmentHeaders = enrichmentHeaders;
		this.sslService = sslService;
		this.enrichmentFields = enrichmentFields;
		this.enrichmentColumns = new int[0];
	}

	@Override
	public void process(InputStream in, OutputStream out) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
			// Analyze the first line which contains column headers
			int columnPosition = 0;
			for (String header : br.readLine().split(",")) {
				// Check if any enrichment headers match any column headers
				for (String enrichmentHeader : enrichmentHeaders.split(",")) {
					if (enrichmentHeader.equals(header)) {
						enrichmentColumns = Arrays.copyOf(enrichmentColumns, enrichmentColumns.length + 1);
						enrichmentColumns[enrichmentColumns.length - 1] = columnPosition;
					}
				}
				columnPosition++;
			}
		} catch (Exception t) {
			//
		}
	}

}
