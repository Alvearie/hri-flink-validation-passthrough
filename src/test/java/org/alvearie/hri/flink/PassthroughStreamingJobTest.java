/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink;

import org.alvearie.hri.flink.PassthroughStreamingJob;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class PassthroughStreamingJobTest {

    private static String TestPassword = "FakePassword";
    private static String TestPasswordArg = "--password=" + TestPassword;
    private static String TestBrokerArg = "--brokers=localhost:9092";
    private static String TestInputTopicVal = "ingest.22.da3.in";
    private static String TestInputTopicArg = "--input=" + TestInputTopicVal;
    private static String TestStandaloneArg = "--standalone";
    private static String TestMgmtUrl = "https://mydomain.com/hri";
    private static String TestMgmtUrlArg = "--mgmt-url=" + TestMgmtUrl;
    private static String TestClientId = "myClientId";
    private static String TestClientIdArg = "--client-id=" + TestClientId;
    private static String TestClientSecret = "myClientSecret";
    private static String TestClientSecretArg = "--client-secret=" + TestClientSecret;
    private static String TestAudience = "myAudience";
    private static String TestAudienceArg = "--audience=" + TestAudience;
    private static String TestOAuthBaseUrl = "https://oauthdomain.com/hri";
    private static String TestOAuthBaseUrlArg = "--oauth-url=" + TestOAuthBaseUrl;

    @Rule
    public final SystemErrRule systemErrRule = new SystemErrRule().enableLog();

    @Test
    public void InvalidPassword() {
        String badPasswordArg = "3737464=FakePassword";
        PassthroughStreamingJob.main(new String[]{TestBrokerArg, TestInputTopicArg, badPasswordArg, TestStandaloneArg});

        assertThat(systemErrRule.getLog(), containsString("Unmatched argument at index"));
        assertThat(systemErrRule.getLog(), containsString(badPasswordArg));
    }

    @Test
    public void missingBrokers() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestStandaloneArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--brokers=<brokers>"));
    }

    @Test
    public void missingMgmtUrl() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestClientIdArg, TestClientSecretArg, TestAudienceArg, TestOAuthBaseUrlArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--mgmt-url=<mgmtUrl>"));
    }

    @Test
    public void missingMgmtClientId() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestMgmtUrlArg, TestClientSecretArg, TestAudienceArg, TestOAuthBaseUrlArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--client-id=<mgmtClientId>"));
    }

    @Test
    public void missingMgmtClientSecret() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestMgmtUrlArg, TestClientIdArg, TestAudienceArg, TestOAuthBaseUrlArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--client-secret=<mgmtClientSecret>"));
    }

    @Test
    public void missingMgmtAudience() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestMgmtUrlArg, TestClientIdArg, TestClientSecretArg, TestOAuthBaseUrlArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--audience=<mgmtAudience>"));
    }

    @Test
    public void missingMgmtOAuthServiceUrl() {
        PassthroughStreamingJob.main(new String[]{TestInputTopicArg, TestPasswordArg, TestMgmtUrlArg, TestClientIdArg, TestClientSecretArg, TestAudienceArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--oauth-url=<oauthServiceBaseUrl>"));
    }

    @Test
    public void missingInputTopic() {
        PassthroughStreamingJob.main(new String[]{TestBrokerArg, TestPasswordArg, TestStandaloneArg});

        assertThat(systemErrRule.getLog(), containsString("Missing required option"));
        assertThat(systemErrRule.getLog(), containsString("--input=<inputTopic>"));
    }

    @Test
    public void invalidTopicValueStandalone() {
        String badInputTopicValue = "ingest-monkey22-noPeriodSeparators";
        String badInputTopicArg = "--input=" + badInputTopicValue;
        PassthroughStreamingJob.main(new String[]{TestBrokerArg, badInputTopicArg, TestPasswordArg, TestStandaloneArg});

        assertThat(systemErrRule.getLog(), containsString("The Input Topic Name " + badInputTopicValue + " is invalid"));
        assertThat(systemErrRule.getLog(), containsString("It must start with \"ingest.\""));
    }

    @Test
    public void invalidTopicValueWithMgmtApi() {
        String badInputTopicValue = "ingest.monkey22-no-in-suffix";
        String badInputTopicArg = "--input=" + badInputTopicValue;
        PassthroughStreamingJob.main(new String[]{TestBrokerArg, badInputTopicArg, TestPasswordArg, TestMgmtUrlArg, TestClientIdArg, TestClientSecretArg, TestAudienceArg, TestOAuthBaseUrlArg});

        assertThat(systemErrRule.getLog(), containsString("The Input Topic Name " + badInputTopicValue + " is invalid"));
        assertThat(systemErrRule.getLog(), containsString("It must end with \".in\""));
    }

}