# HRI Flink Validation Passthrough

The Alvearie Health Record Ingestion service: a common 'Deployment Ready Component' designed to serve as a “front door” for data for cloud-based solutions. See our [documentation](https://pages.github.com/Alvearie/HRI) for more details.

This repo contains the code for the HRI Flink Validation Passthrough Job of the HRI, which does not inspect the record payloads to validate any particular data format. 

Note: This software uses Apache Flink (https://flink.apache.org/) for streaming data and is written in Java (1.8).

## Communication
* Please [join](https://alvearie.io/contributions/requestSlackAccess/) our Slack channel for further questions: `#health-record-ingestion`
* Please see recent contributors or [maintainers](MAINTAINERS.md)

## Getting Started

### Prerequisites
* Java 1.8 - IBM requires the use of AdoptOpenJDK java distribution which you can download from [this site](https://adoptopenjdk.net/?variant=openjdk8) or install using a package manager like `homebrew` for mac
* Java/Scala IDE (Optional) - we use IntelliJ, but it requires a licensed version.
* Ruby (Optional) - required for integration tests. See [testing](test/README.md) for more details.
* IBM Cloud CLI (Optional) - useful for local testing. Installation [instructions](https://cloud.ibm.com/docs/cli?topic=cloud-cli-getting-started).

### Building
From the base directory, run `./gradlew clean build`. This will download dependencies and run all the unit tests. 

This depends on the `hri-flink-pipeline-core` [GitHub repo](https://github.com/Alvearie/hri-flink-pipeline-core) and its published packages.

```
hri-flink-validation-passthrough % ./gradlew clean build
Starting a Gradle Daemon (subsequent builds will be faster)

BUILD SUCCESSFUL in 42s
14 actionable tasks: 14 executed

```

## CI/CD
GitHub actions is used for CI/CD. It runs unit tests, builds the code, and then runs integration tests on our Flink cluster using Event Streams and a Management API instance running in Kubernetes.

Each branch uses its own topics, so different builds don't interfere with each other. Integration tests will clean up after themselves cancelling the Flink job, deleting the job jar, and the Event Streams topics.

The Flink logs are available for trouble shooting. They can be viewed in the Flink UI or the Kubernetes logs. The logs for all jobs are combined together, so you may need to search to a specific time frame or look for specific keywords.

## Releases
Releases are created by creating Git tags, which trigger a GitHub Actions build that publishes a release version in GitHub packages, see [Overall strategy](https://github.com/Alvearie/HRI/wiki/Overall-Project-Branching,-Test,-and-Release-Strategy) for more details.

## Code Overview

### Classes
There are two primary classes:
- PassthroughValidator - implements the Validator interface that lets all records 'passthrough.' In other words, it is a validator that does not inspect the contents of the record and for all records, responds that they are valid.

- PassthroughStreamingJob - constructs an HRI Flink Validation job with a PassthroughValidator and executes the streaming job. The standard operational mode used in all production jobs uses the [Management Api](https://github.com/Alvearie/hri-mgmt-api).

### Tests
This repository contains both unit tests and end-to-end integration tests.

- PassthroughValidatorTest and PassthroughStreamingJobTest contain unit tests for their respective classes.
- The passthrough.jobtest package contain examples of end-to-end streaming job tests. 

#### Test Coverage
The HRI team requires that your code has at least 90% unit test code coverage. Anything less will likely not be accepted as a new contribution.

The build automatically creates a JaCoCo Test Report that contains test coverage percentages and a coverage file at `hri-flink-validation-passthrough/build/reports/jacoco/test/html/index.html`. 

## Contribution Guide
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.