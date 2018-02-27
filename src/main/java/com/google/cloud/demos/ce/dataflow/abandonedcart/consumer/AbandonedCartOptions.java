/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.demos.ce.dataflow.abandonedcart.consumer;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * AbandonedCartOptions supported by the exercise pipelines.
 */
public interface AbandonedCartOptions extends DataflowPipelineOptions{


    @Description("Pub/Sub subscription to read from. Used if --input is empty.")
    @Validation.Required
    String getSubscription();
    void setSubscription(String value);

    @Description("Big Query Abandoned Table output")
    @Validation.Required
    String getOutputAbandonedTable();
    void setOutputAbandonedTable(String value);

    @Description("Big Query Table output Raw Events")
    @Validation.Required
    String getOutputRawTable();
    void setOutputRawTable(String value);

}
