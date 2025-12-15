package org.openprojectx.spark.stream;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

@Plugin(name = "KafkaDataLossAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public class KafkaDataLossAppender extends AbstractAppender {

    protected KafkaDataLossAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                                    boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions);
    }

    @PluginFactory
    public static KafkaDataLossAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout) {

        if (layout == null) {
            layout = PatternLayout.newBuilder().withPattern("%m").build();
        }
        return new KafkaDataLossAppender(name, filter, layout, true);
    }

    @Override
    public void append(LogEvent event) {
        String msg = event.getMessage().getFormattedMessage();

        // Spark Kafka loss warnings contain these
        if (msg.contains("Some data may be lost")
                || msg.contains("Cannot fetch offset")
                || msg.contains("Skip missing records")) {

            System.err.println("[DATA LOSS DETECTED] " + msg);

            // TODO: your custom logic
            // sendToElasticsearch(msg);
            // sendToKafka("monitoring.dataLoss", msg);
            // writeToS3("spark/data-loss/" + System.currentTimeMillis(), msg);
        }
    }
}
