/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink;

import org.alvearie.hri.api.BatchLookup;
import org.alvearie.hri.flink.core.BaseValidationJob;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import picocli.CommandLine;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "validate",
        synopsisSubcommandLabel = "COMMAND",
        mixinStandardHelpOptions = true,
        version = "1.0",
        description = "HRI Flink passthrough validation job"
) public class PassthroughStreamingJob implements Callable<Integer> {
    boolean isTestJob = false;
    RichParallelSourceFunction notificationSrc;
    RichParallelSourceFunction testRecsSrc;
    RichSinkFunction validRecsSink;
    RichSinkFunction invalidRecsSink;
    RichSinkFunction countRecsSink;
    BatchLookup batchLookup;

    @CommandLine.Option(names = {"-b", "--brokers"}, split = ",", description = "Comma-separated list of Event Streams (Kafka) brokers", required = true)
    private String[] brokers;

    @CommandLine.Option(names = {"-p", "--password"}, description = "IBM Cloud Event Streams password", required = false)
    private String password;

    @CommandLine.Option(names = {"-i", "--input"}, description = "IBM Cloud Event Streams (Kafka) input topic", required = true)
    private String inputTopic;

    @CommandLine.Option(names = {"-d", "--batch-completion-delay"}, defaultValue = "300000", description = "Amount of time to wait in milliseconds for extra records before completing a batch. Default ${DEFAULT-VALUE}")
    private Long batchCompletionDelay;

    // This says the parameters defined in the class are exclusive. Only one of them can be defined.
    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    private StandaloneOrMgmtParams standaloneOrMgmtParams;

    public PassthroughStreamingJob() {}

    public PassthroughStreamingJob(RichParallelSourceFunction notificationSrc, RichParallelSourceFunction testRecsSrc,
                                   RichSinkFunction validRecsSink, RichSinkFunction invalidRecsSink,
                                   RichSinkFunction countRecsSink, BatchLookup batchLookup, Long batchCompletionDelay) {
        this.isTestJob = true;
        this.notificationSrc = notificationSrc;
        this.testRecsSrc = testRecsSrc;
        this.validRecsSink = validRecsSink;
        this.invalidRecsSink = invalidRecsSink;
        this.countRecsSink = countRecsSink;
        this.batchLookup = batchLookup;
        this.batchCompletionDelay = batchCompletionDelay;
    }

    private static class StandaloneOrMgmtParams {
        @CommandLine.Option(names = {"--standalone"}, description = "Deploys in standalone mode, where the Management API is NOT used to complete or fail batches", required = true)
        private boolean standalone;

        // This says the parameters defined in the class are inclusive. All of them must be defined.
        @CommandLine.ArgGroup(exclusive = false)
        private MgmtParams mgmtParams;
    }

    private static class MgmtParams {
        @CommandLine.Option(names = {"-m", "--mgmt-url"}, description = "Base Url for the Management API, e.g. https://68d40cd8.us-south.apigw.appdomain.cloud/hri", required = true)
        private String mgmtUrl;

        @CommandLine.Option(names = {"-c", "--client-id"}, description = "Client ID for getting OAuth access tokens", required = true)
        private String mgmtClientId;

        @CommandLine.Option(names = {"-s", "--client-secret"}, description = "Client secret for getting OAuth access tokens", required = true)
        private String mgmtClientSecret;

        @CommandLine.Option(names = {"-a", "--audience"}, description = "Audience for getting OAuth access tokens", required = true)
        private String mgmtAudience;

        @CommandLine.Option(names = {"-o", "--oauth-url"}, description = "Base Url for the OAuth service", required = true)
        private String oauthServiceBaseUrl;
    }

    public Integer call() {

        // The HRI Flink Validation job is constructed with a PassthroughValidator. See the PassthroughValidator/Test
        // classes for examples of how to create and test a custom Validator class.
        // Note: a lambda function could be passed in, but it must be serializable, which is not recommended per
        // https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html#serialization

        if (this.isTestJob) {
            PassthroughValidator validator = new PassthroughValidator();
            BaseValidationJob baseValidationJob = new BaseValidationJob(
                    validator, notificationSrc, testRecsSrc, validRecsSink, invalidRecsSink, countRecsSink, batchLookup, batchCompletionDelay);

            baseValidationJob.startJob("End-to-End TEST Job-Passthrough Schema Validation");

            return CommandLine.ExitCode.OK;
        }

        BaseValidationJob validationJob;

        if (standaloneOrMgmtParams.standalone) {
            // This constructor creates the job in 'standalone' mode, which means it does not contact the Management API.
            // This mode is only used for automated functional validation testing and not intended for using in production.
            validationJob = new BaseValidationJob(inputTopic, brokers, password, new PassthroughValidator(), batchCompletionDelay);
        } else {
            // This constructor creates the job in the standard operational mode, which uses the Management API.
            // Use this constructor for all production jobs.
            validationJob = new BaseValidationJob(inputTopic, brokers, password, new PassthroughValidator(),
                    standaloneOrMgmtParams.mgmtParams.mgmtUrl, standaloneOrMgmtParams.mgmtParams.mgmtClientId,
                    standaloneOrMgmtParams.mgmtParams.mgmtClientSecret, standaloneOrMgmtParams.mgmtParams.mgmtAudience,
                    standaloneOrMgmtParams.mgmtParams.oauthServiceBaseUrl, batchCompletionDelay);
        }

        // Start the streaming job
        validationJob.startJob("Passthrough");

        return CommandLine.ExitCode.OK;
    }

    public static void main(String[] args) {
        new CommandLine(new PassthroughStreamingJob()).execute(args);
    }
}
