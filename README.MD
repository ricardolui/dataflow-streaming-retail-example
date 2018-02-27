# Real Time Analytics for Retail with Google Cloud - Cart Abandonment Use Case
 
This project is a sample demo built on top of Google Cloud Platform managed services.
We separate the source code into 2 parts:

#### Producer
Producer is the code responsible for generating all the real time events, simulating devices browsing the website and all the Click Stream.
It will send it directly to Google Cloud PubSub. 
Producer project is here
https://github.com/ricardolui/pubsub-publisher-retail


#### Consumer
The consumer code is developed using Apache Beam SDK, and can be deployed directly on Google Cloud Dataflow as a runner. 
The concept of Apache Beam brings in decoupling the Beam code from the Runner. 
Google Cloud Dataflow runner has some optimization and runs as a Managed Service, so users don't need to worry about provisioning infrastructure, scaling, etc.
For this particular example, all the click stream goes into PubSub and the Apache Beam pipeline will divided into 2 flows:
* 1-) Archive the raw message into Google BigQuery (Google Cloud Data Warehousing solution)
* 2-) Runs a sliding window analysis which will collect all users that abandoned their carts with a simple example algorithm. After we filter all the users, 



## Set up your environment

### Tools

1.  Install [Java 8](https://java.com/fr/download/)
1.  Install Maven (for
    [windows](https://maven.apache.org/guides/getting-started/windows-prerequisites.html),
    [mac](http://tostring.me/151/installing-maven-on-os-x/),
    [linux](http://maven.apache.org/install.html))
1.  Install Google [Cloud SDK](https://cloud.google.com/sdk/)
1.  Install an IDE such as [Eclipse](https://eclipse.org/downloads/) or
    [IntelliJ](https://www.jetbrains.com/idea/download/) (optional)
1.  To test your installation, open a terminal window and type:

    ```shell
    $ java -version
    $ mvn --version
    $ gcloud --version
    ```

### Google Cloud

1.  Go to https://cloud.google.com/console.
1.  Enable billing and create a project.
1.  Enable Google Dataflow API, Google Cloud PubSub and BigQuery API.
1.  Create a GCS bucket in your project as a staging location.
1.  Create a BigQuery dataset in your project.

### Download the code

1.  Clone the github repository

    ```shell
    $ git clone https://github.com/ricardolui/dataflow-streaming-retail-example
    $ cd dataflow-streaming-retail-example
    ```



### Running the code

1.  Compile and run the pipeline. Note you need to replace 
    YOUR-PROJECT (your GCP project name)
    YOUR-SUBSCRIPTION (for PubSub Topic that your producer will be posting messages)
    YOUR-BIGQUERY-DATASET (your BigQuery Dataset Name)
    YOUR-RAW-TABLE-NAME (for the raw events archival)
    YOUR-ABANDONED-TABLE-NAME (your BigQuery Abandoned Table Name)
    MAX_NUM_WORKERS (maximum number of workers for Autoscaling) 
    with values matching your environment.
    
    Optional:
    "--workerMachineType=YOUR_SPECIFIC_INSTANCE_TYPE"
    

    ```shell
    $ mvn compile exec:java \
       -Dexec.mainClass=com.google.cloud.demos.ce.dataflow.abandonedcart.consumer.PubSubBigQuerySlidingWindowAbandonedCart \
       -Dexec.args="--project=YOUR-PROJECT \
                    --runner=DataflowRunner \
                    --subscription=YOUR-SUBSCRIPTION \
                    --outputAbandonedTable=YOUR-PROJECT:YOUR-BIGQUERY-DATASET.YOUR-ABANDONED-TABLE-NAME \
                    --outputRawTable=YOUR-PROJECT:YOUR-BIGQUERY-DATASET.YOUR-RAW-TABLE-NAME \
                    --maxNumWorkers=MAX_NUM_WORKERS"
    ```
    

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

