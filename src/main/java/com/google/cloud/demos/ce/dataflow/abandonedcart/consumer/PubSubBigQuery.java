package com.google.cloud.demos.ce.dataflow.abandonedcart.consumer;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.List;


public class PubSubBigQuery {

    /**
     * Defines the BigQuery schema used for the output.
     */
    static TableSchema getSchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("customer").setType("STRING"));
        fields.add(new TableFieldSchema().setName("useragent").setType("STRING"));
        fields.add(new TableFieldSchema().setName("page").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }


    public static void main(String[] args) throws Exception {

        AbandonedCartOptions abandonedCartOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(AbandonedCartOptions.class);
        // Enforce that this pipeline is always run in streaming mode
        abandonedCartOptions.setStreaming(true);

        String rawEvents = abandonedCartOptions.getOutputRawTable();
        String subscription = abandonedCartOptions.getSubscription();

        // update saving to partitioned table look at documentation
        // TableDestination
        // https://beam.apache.org/documentation/sdks/javadoc/2.2.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html
        Pipeline pipeline = Pipeline.create(abandonedCartOptions);
        pipeline.apply(PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
                .apply("ProcessPubSub", ParDo.of(new ParsePubsubFormat()))
                .apply(BigQueryIO.writeTableRows()
                        .to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
                            public TableDestination apply(ValueInSingleWindow<TableRow> value) {
                                // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.

                                String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
                                        .withZone(DateTimeZone.UTC).print(System.currentTimeMillis());
                                return new TableDestination(
                                        rawEvents + "_only_" + dayString, // Table spec
                                        "Output for day " + dayString // Table description
                                );
                            }
                        })
                        .withSchema(PubSubBigQuery.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();

    }
}
