package com.viadeo.hadoop.metrics2.sinks;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MsInfo;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CarbonSink implements MetricsSink {

    private static final String HOSTNAME_KEY = "collector";
    private static final String PORT_KEY = "port";
    private DatagramSocket sock;
    private InetAddress addr;
    private int port;
    // a key with a NULL value means ALL
    private Map<String, Set<String>> useTagsMap = new HashMap<String, Set<String>>();

    @Override
    public void init(SubsetConfiguration conf) {
        try {
            sock = new DatagramSocket();
            addr = InetAddress.getByName(conf.getString(HOSTNAME_KEY));
            port = conf.getInt(PORT_KEY);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @InterfaceAudience.Private
    public void appendPrefix(MetricsRecord record, StringBuilder sb) {
        String contextName = record.context();
        Collection<MetricsTag> tags = record.tags();
        if (useTagsMap.containsKey(contextName)) {
            Set<String> useTags = useTagsMap.get(contextName);
            for (MetricsTag t : tags) {
                if (useTags == null || useTags.contains(t.name())) {

                    // the context is always skipped here because it is always added

                    // the hostname is always skipped to avoid case-mismatches
                    // from different DNSes.

                    if (t.info() != MsInfo.Context && t.info() != MsInfo.Hostname && t.value() != null) {
                        sb.append('.').append(t.name()).append('=').append(t.value());
                    }
                }
            }
        }
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        DataOutputStream dos = null;
        String recordName = record.name();
        String contextName = record.context();
        long timestamp = record.timestamp();

        StringBuilder sb = new StringBuilder();
        sb.append(contextName);
        sb.append('.');
        sb.append(recordName);

        appendPrefix(record, sb);

        String groupName = sb.toString();
        sb.append('.');


        Collection<AbstractMetric> metrics = (Collection<AbstractMetric>) record.metrics();
        if (metrics.size() > 0) {
            try {
                String message = "";
                // we got metrics. so send the latest
                for (AbstractMetric metric : record.metrics()) {
                    sb.append(metric.name());
                    String name = sb.toString();

                    message.concat(groupName + '.' + name + ' ' + metric.value().toString() + timestamp + "\n".getBytes());

                }
                byte[] data = message.getBytes();

                DatagramPacket packet = new DatagramPacket(data, data.length, addr, port);
                sock.send(packet);

            } catch (IOException io) {
                throw new MetricsException("Failed to putMetrics", io);
            }
        }

    }

    @Override
    public void flush() {
        //Nothing to flush
    }


    public void close() {
        if (sock != null) {
            sock.close();
        }
    }
}