package com.google.cloud.demos.ce.dataflow.abandonedcart.consumer;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.demos.ce.dataflow.abandonedcart.AbonandonedCartsVariables;
import com.google.cloud.demos.ce.dataflow.abandonedcart.PageView;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class PubSubBigQuerySlidingWindowAbandonedCart {


    private static final Logger LOG = LoggerFactory.getLogger(PubSubBigQuerySlidingWindowAbandonedCart.class);
    public static final int WINDOW_DURATION = 5;
    public static final int SLIDING_UPDATE = 30;
    public static final int ALLOWED_LATENESS = 1;

    /**
     * Defines the BigQuery schema used for the raw streaming events.
     */
    static TableSchema getEventsSchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("customer").setType("STRING"));
        fields.add(new TableFieldSchema().setName("useragent").setType("STRING"));
        fields.add(new TableFieldSchema().setName("page").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    /**
     * Defines the BigQuery schema used for the raw streaming events.
     */
    static TableSchema getAbandonedCartsSchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("useragent").setType("STRING"));
        fields.add(new TableFieldSchema().setName("customer").setType("STRING"));
        TableFieldSchema itemsSchema = new TableFieldSchema().setName("items").setType("RECORD").setMode("REPEATED");
        ArrayList<TableFieldSchema> arrayFields = new ArrayList<TableFieldSchema>();
        arrayFields.add(new TableFieldSchema().setName("item").setType("STRING"));
        itemsSchema.setFields(arrayFields);
        fields.add(itemsSchema);
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }


    //Transformation from PubSubFormatToPageViewUsingJson
    static class ParsePubsubFormatPageView extends DoFn<PubsubMessage, PageView> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            Gson gson = new Gson();
            PubsubMessage msg = c.element();
            byte[] data = msg.getPayload();
            String jsonString = new String(data);
            PageView pageView = gson.fromJson(jsonString, PageView.class);
            c.output(pageView);
        }
    }


    static class FilterOutCustomersWithoutCheckout extends DoFn<KV<String, Iterable<PageView>>, KV<String, Iterable<PageView>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            Iterable<PageView> pageViews = c.element().getValue();
            boolean hasCheckout = false;
            for (PageView pageView : pageViews) {
                if (pageView.getPage().equals(AbonandonedCartsVariables.CHECKOUT_PAGE)) {
//                    System.out.println("Tem Checkout pro CustomerId" + pageView.getCustomer());
                    hasCheckout = true;
                }
            }
            //only output those who does not have checkout page
            if (!hasCheckout) {
                c.output(KV.of(c.element().getKey(), c.element().getValue()));
            }
            else
            {
                LOG.debug("Customer: "+ c.element().getKey() + " has checkout");
            }
        }
    }

    static class FilterOnlyCartCustomerOnFirstHalfWindow extends DoFn<KV<String, Iterable<PageView>>, KV<String, Iterable<PageView>>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            Iterable<PageView> pageViews = c.element().getValue();
            ArrayList<PageView> outputPageView = new ArrayList<PageView>();
            boolean hasCart = false;
            for (PageView pageView : pageViews) {
                // Verify if its in the first half of the window, between 20 and 10 minutes before now
                DateTime dateTime10 = new DateTime();
                dateTime10.minusMinutes(10);

                DateTime dateTime20 = new DateTime();
                dateTime20.minusMinutes(20);

                LOG.debug("PageViewTimestamp: " + pageView.getTimestamp());
                LOG.debug("PageViewTimestamp20: " + dateTime20.toDate());
                LOG.debug("PageViewTimestamp10: " + dateTime10.toDate());
                LOG.debug("Page: " + pageView.getPage());

                //timestamp is after start of the window and before half and has visited the cart
                if (pageView.getPage().equals(AbonandonedCartsVariables.CART_PAGE)) {
//                    if (pageView.getTimestamp().after(dateTime20.toDate())
//                            && pageView.getTimestamp().before(dateTime10.toDate())
//                            && pageView.getPage().equals(AbonandonedCartsVariables.CART_PAGE)) {
                    hasCart = true;
                    outputPageView.add(pageView);

//                    }
                }
            }
            if (hasCart) {
                //setting output of only one page
                c.output(KV.of(c.element().getKey(), outputPageView));
            }
        }
    }

    static class ConvertFilteredCustomersToBQTableRow extends DoFn<KV<String, Iterable<PageView>>, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            Iterable<PageView> pageViews = c.element().getValue();
            int totalPages = 0;

            boolean processedPage = false;
            for (PageView pageView : pageViews) {
                totalPages++;

                if (pageView.getItems() != null && pageView.getItems().size() > 0 && !processedPage) {
                    //Create TableRow Here to write and return
                    SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    TableRow row = new TableRow();
                    row.set("timestamp", dateFormatLocal.format(pageView.getTimestamp()));
                    row.set("customer", pageView.getCustomer());
                    row.set("useragent", pageView.getUseragent());

                    List<TableRow> itemsList = new ArrayList<TableRow>();

                    for (String item : pageView.getItems()) {
                        TableRow rowItem = new TableRow();
                        rowItem.set("item", item);
                        itemsList.add(rowItem);
                    }
                    row.set("items", itemsList);
                    c.output(row);
                    processedPage = true;
                }
            }
            LOG.debug("Customer: " + c.element().getKey() + " TotalPages: " + totalPages);
        }
    }


    public static void main(String[] args) throws Exception {

        AbandonedCartOptions abandonedCartOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(AbandonedCartOptions.class);
        // Enforce that this pipeline is always run in streaming mode
        abandonedCartOptions.setStreaming(true);

        String tableAbandoned =  abandonedCartOptions.getOutputAbandonedTable();
        String tableRaw = abandonedCartOptions.getOutputRawTable();
        String subscription = abandonedCartOptions.getSubscription();

        // update saving to partitioned table look at documentation
        // TableDestination
        // https://beam.apache.org/documentation/sdks/javadoc/2.2.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html
        Pipeline pipeline = Pipeline.create(abandonedCartOptions);
        PCollection<PubsubMessage> rawPubSub = pipeline.apply(PubsubIO.readMessagesWithAttributes().fromSubscription(subscription).withTimestampAttribute(AbonandonedCartsVariables.TIMESTAMP_ATTRIBUTE));
        //without timestamp on Pubsub from Dataflow
        PCollection<PageView> pageViews = rawPubSub.apply("ConvertPubSub2PageView", ParDo.of(new ParsePubsubFormatPageView()));
        //Transform PageView and include map to Key for Customer and PageView as Value
        pageViews.apply("KeyPageViewPerCustomer", WithKeys.of((PageView page) -> page.getCustomer()).withKeyType(TypeDescriptor.of(String.class)))
        //Setup the SlidingWindow
                .apply("SlidingWindow", Window.<KV<String, PageView>>into(SlidingWindows.of(Duration.standardMinutes(WINDOW_DURATION)).every(Duration.standardSeconds(SLIDING_UPDATE)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane())
                        .withAllowedLateness(Duration.standardMinutes(ALLOWED_LATENESS))
                        .accumulatingFiredPanes())
                //Group by Key (Customer and PageView)
                .apply("GroupByKey", GroupByKey.<String, PageView>create())
                // Filter Dataset to remove all customer ID with that have checkout page
                .apply("FilterOutCheckoutCustomers", ParDo.of(new FilterOutCustomersWithoutCheckout()))
                // Filter based on time
                .apply("FilterOnlyCartCustomerOnFirstHalfWindow", ParDo.of(new FilterOnlyCartCustomerOnFirstHalfWindow()))
                // Save to BigQueryTable
                .apply("ConvertFilteredCustomersToBQTableRows", ParDo.of(new ConvertFilteredCustomersToBQTableRow()))
                .apply(BigQueryIO.writeTableRows()
                        .to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
                            public TableDestination apply(ValueInSingleWindow<TableRow> value) {
                                // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.

                                String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
                                        .withZone(DateTimeZone.UTC).print(System.currentTimeMillis());
                                return new TableDestination(
                                        tableAbandoned + dayString, // Table spec
                                        "Output for day " + dayString // Table description
                                );
                            }
                        })
                        .withSchema(PubSubBigQuerySlidingWindowAbandonedCart.getAbandonedCartsSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        rawPubSub.apply("ProcessPubSub", ParDo.of(new ParsePubsubFormat()))
                .apply(BigQueryIO.writeTableRows()
                        .to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
                            public TableDestination apply(ValueInSingleWindow<TableRow> value) {
                                // The cast below is safe because CalendarWindows.days(1) produces IntervalWindows.

                                String dayString = DateTimeFormat.forPattern("yyyy_MM_dd")
                                        .withZone(DateTimeZone.UTC).print(System.currentTimeMillis());
                                return new TableDestination(
                                        tableRaw + dayString, // Table spec
                                        "Output for day " + dayString // Table description
                                );
                            }
                        })
                        .withSchema(PubSubBigQuerySlidingWindowAbandonedCart.getEventsSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();

    }
}
