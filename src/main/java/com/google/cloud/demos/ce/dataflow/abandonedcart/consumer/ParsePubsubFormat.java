package com.google.cloud.demos.ce.dataflow.abandonedcart.consumer;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.demos.ce.dataflow.abandonedcart.PageView;
import com.google.gson.Gson;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * ParsePubSubFormat
 * RAW write to BQTableRow events table
 */
public class ParsePubsubFormat extends DoFn<PubsubMessage, TableRow> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParsePubsubFormat.class);

    @ProcessElement
    public void processElement(ProcessContext c) {

        System.out.println("Entrou");
        Gson gson = new Gson();

        PubsubMessage msg = c.element();
        byte[] data = msg.getPayload();
        String jsonString = new String(data);
        PageView pageView = gson.fromJson(jsonString, PageView.class);

        Date datetime = Calendar.getInstance().getTime();
        SimpleDateFormat dateFormatLocal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TableRow row = new TableRow();
        row.set("timestamp", dateFormatLocal.format(datetime));
        row.set("customer", pageView.getCustomer());
        row.set("useragent", pageView.getUseragent());
        row.set("page", pageView.getPage());
        c.output(row);

    }
}