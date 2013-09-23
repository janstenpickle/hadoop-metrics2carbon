package org.apache.hadoop.metrics2.sinks;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.tools.ExponentialBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CarbonSink implements MetricsSink {

	private static final Logger logger = LoggerFactory.getLogger(CarbonSink.class);


	private static final String HOSTNAME_KEY = "amqp.host";
	private static final String PORT_KEY = "amqp.port";
	private static final String EXCHANGE_NAME_KEY = "amqp.exchange.name";
	private static final String EXCHANGE_DURABLE_KEY = "amqp.exchange.durable";
	private static final String USERNAME_KEY = "amqp.username";
	private static final String PASSWORD_KEY = "amqp.password";
	private static final String ROUTING_KEY = "amqp.routing.key";
	private static final String VHOST_KEY = "amqp.vhost";
	private static final String PREFIX_KEY = "prefix";
	private static final String ZEROS_AS_NULL_KEY = "zeros.as.null";
	private static final String BACKOFF_BASE_SLEEP_TIME = "base.sleep.time";
	private static final String RETRIES = "amqp.retries";


	private ConnectionFactory factory = new ConnectionFactory();
	private Connection connection;
	private Channel channel;
	private String routingKey;
	private String exchangeName;
	private String prefix;
	private Boolean zerosAsNull;
	// a key with a NULL value means ALL
	private Map<String, Set<String>> useTagsMap = new HashMap<String, Set<String>>();
	private SubsetConfiguration conf;
	private ExponentialBackoff exponentialBackoff;
	private int retries;

	@Override
	public void init(SubsetConfiguration conf) {
		this.conf = conf;
		int baseSleepTime = conf.getInt(BACKOFF_BASE_SLEEP_TIME, 500);
		exponentialBackoff = new ExponentialBackoff(baseSleepTime);
		retries = conf.getInt(RETRIES, 15);

		connectAmqp();
	}

	private void connectAmqp() {
		try {
			String addr = conf.getString(HOSTNAME_KEY);
			int port = conf.getInt(PORT_KEY, 5672);
			String username = conf.getString(USERNAME_KEY, null);
			String password = conf.getString(PASSWORD_KEY, null);
			exchangeName = conf.getString(EXCHANGE_NAME_KEY, "metrics");
			boolean exchangeDurable = conf.getBoolean(EXCHANGE_DURABLE_KEY, true);
			routingKey = conf.getString(ROUTING_KEY, "#");
			String vhost = conf.getString(VHOST_KEY, "/");
			prefix = conf.getString(PREFIX_KEY, "hadoop");
			zerosAsNull = conf.getBoolean(ZEROS_AS_NULL_KEY, true);

			factory.setHost(addr);
			factory.setPort(port);
			factory.setVirtualHost(vhost);
			if (username != null) {
				factory.setUsername(username);
				if (username != null) {
					factory.setPassword(password);
				}
			}
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(exchangeName, "topic", exchangeDurable);

			logger.info("AMQP Connection to " + addr + " established");

		} catch (IOException io) {
			logger.warn("Could not establish AMQP connection, will retry", io);
		}
	}


	private void disconnectAmqp() {
		try {
			connection.close();
		} catch (Exception e) {
			logger.warn("Could not close AMQP connection", e);
		}
	}

	@Override
	public void putMetrics(MetricsRecord record) {

		//We only need EPOCH time to be 10 digits
		long timestamp = record.timestamp() / 1000;

		Collection<MetricsTag> tags = (Collection<MetricsTag>) record.tags();

		String prefix = this.prefix;

		for (MetricsTag t : tags) {
			logger.debug("Tag " + t.name() + " desc " + t.description() + " value " + t.value());
			if (t.name().equals("context")) {
				prefix = prefix + "." + t.value();
			}
		}

		Collection<Metric> metrics = (Collection<Metric>) record.metrics();
		if (metrics.size() > 0) {
			StringBuffer message = new StringBuffer();
			// we got metrics. so send the latest
			for (Metric metric : record.metrics()) {

				float value = metric.value().floatValue();
				//do not send the value if it is zero and config is set to do so
				if (value != 0 && zerosAsNull) {
					value = Math.round(value * 1000) / 1000;

					message.append(prefix + '.' + metric.name() + ' ' + value + ' ' + timestamp + "\n");

					logger.debug("Metric " + prefix + metric.name() + " value: " + value);

				}

			}
			for (int i = 0; i < retries; i++) {
				try {

					logger.debug("Publishing metric " + message);
					channel.basicPublish(exchangeName, routingKey, null, message.toString().getBytes());

					break;

				} catch (Exception e) {
					long sleep = exponentialBackoff.getSleepTimeMs();
					logger.warn("Could not publish metric, attempting to restart AMQP connection, " +
							"backing off for " + sleep + "ms, retries remaining: "+(retries - i), e);
					disconnectAmqp();
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException ie) {
						logger.error("Could not sleep thread", ie);
					}
					connectAmqp();
				}
			}
		}

	}


	@Override
	public void flush() {
		//Nothing to flush
	}


	public void close() throws IOException {
		disconnectAmqp();
	}
}