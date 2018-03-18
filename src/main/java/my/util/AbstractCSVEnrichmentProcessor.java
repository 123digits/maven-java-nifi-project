package my.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

public abstract class AbstractCSVEnrichmentProcessor extends AbstractProcessor {

	// Relationships
	private static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Success relationship").build();
	private static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
			.description("Failure relationship").build();
	private static final Set<Relationship> RELATIONSHIPS = new HashSet<>(Arrays.asList(SUCCESS, FAILURE));

	// Properties
	private static final PropertyDescriptor ENRICHMENT_HEADERS = new PropertyDescriptor.Builder()
			.name("Enrichment Headers").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	private static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder().name("SSL Context Service")
			.description(
					"The SSL Context Service to use in order to secure the server. If specified, the server will accept only HTTPS requests; "
							+ "otherwise, the server will accept only HTTP requests")
			.required(false).identifiesControllerService(RestrictedSSLContextService.class).build();
	private static final List<PropertyDescriptor> PROPERTIES = Arrays.asList(ENRICHMENT_HEADERS, SSL_CONTEXT);

	public abstract byte[] getCsvHeaders(String line, String enrichmentHeaders);

	public abstract byte[] getCsvRow(String line, SSLContextService sslService) throws IOException;

	@Override
	public final Set<Relationship> getRelationships() {
		return RELATIONSHIPS;
	}

	@Override
	protected final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return PROPERTIES;
	}

	@Override
	public final void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// Get a pending file
		FlowFile file = session.get();

		// Do nothing if there is no pending file
		if (Objects.isNull(file)) {
			return;
		}

		// Properties
		String enrichmentHeaders = context.getProperty(ENRICHMENT_HEADERS).getValue();
		SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);

		// another idea...
		FlowFile newFile2 = session.create(file);
		try {
			session.write(newFile2, new StreamCallback() {

				@Override
				public void process(InputStream in, OutputStream out) throws IOException {
					try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
						// analyze CSV column headers
						String line = br.readLine();
						if (Objects.nonNull(line)) {
							out.write(getCsvHeaders(line, enrichmentHeaders));
						}

						// analyze CSV data rows
						while (Objects.nonNull(line = br.readLine())) {
							out.write(getCsvRow(line, sslService));
						}
					} catch (Exception t) {
						//
					}
				}

			});
			session.transfer(newFile2, SUCCESS);
		} catch (Exception e) {
			getLogger().error("Couldn't enrich file", e);
			session.remove(newFile2);
			session.transfer(file, FAILURE);
		} finally {
			session.commit();
		}
	}
}
