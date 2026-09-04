package com.atlasflow.aws;

import com.atlasflow.config.AtlasFlowProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

/**
 * AWS SDK client beans, shared between the control plane and the worker
 * (both are Spring components in this single deployable; see
 * docker-compose.yml for how the same jar runs as two containers under
 * different profiles/entrypoints).
 *
 * When {@code atlasflow.aws-endpoint-override} is set (as it is in
 * docker-compose.yml, pointing at the LocalStack container), every client
 * talks to LocalStack instead of real AWS -- this is what lets the whole
 * platform be developed and demoed without an AWS account or any AWS
 * spend. In a real deployment that property is simply left unset, and the
 * SDK's default credential/region provider chain (IAM role, environment
 * variables, etc.) takes over exactly as it would for any AWS SDK
 * application.
 *
 * Each bean method applies the LocalStack override inline rather than
 * through a shared generic helper: AWS SDK v2's builder types use a
 * self-referential generic bound (e.g. {@code DynamoDbClientBuilder extends
 * AwsClientBuilder<DynamoDbClientBuilder, DynamoDbClient>}) for fluent
 * chaining, which is simple to use directly per-builder but easy to get
 * subtly wrong when abstracted behind a wildcarded shared type -- not a
 * risk worth taking in code this project can't compile-check locally (see
 * docs/local-development.md).
 */
@Configuration
@EnableConfigurationProperties(AtlasFlowProperties.class)
public class AwsClientConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(AtlasFlowProperties properties) {
        DynamoDbClient.Builder builder = DynamoDbClient.builder().region(Region.of(properties.getAwsRegion()));
        if (hasLocalEndpointOverride(properties)) {
            builder = builder
                    .endpointOverride(URI.create(properties.getAwsEndpointOverride()))
                    .credentialsProvider(localStackCredentials());
        }
        return builder.build();
    }

    @Bean
    public SqsClient sqsClient(AtlasFlowProperties properties) {
        SqsClient.Builder builder = SqsClient.builder().region(Region.of(properties.getAwsRegion()));
        if (hasLocalEndpointOverride(properties)) {
            builder = builder
                    .endpointOverride(URI.create(properties.getAwsEndpointOverride()))
                    .credentialsProvider(localStackCredentials());
        }
        return builder.build();
    }

    @Bean
    public SnsClient snsClient(AtlasFlowProperties properties) {
        SnsClient.Builder builder = SnsClient.builder().region(Region.of(properties.getAwsRegion()));
        if (hasLocalEndpointOverride(properties)) {
            builder = builder
                    .endpointOverride(URI.create(properties.getAwsEndpointOverride()))
                    .credentialsProvider(localStackCredentials());
        }
        return builder.build();
    }

    private static boolean hasLocalEndpointOverride(AtlasFlowProperties properties) {
        return properties.getAwsEndpointOverride() != null && !properties.getAwsEndpointOverride().isBlank();
    }

    private static StaticCredentialsProvider localStackCredentials() {
        // LocalStack does not validate credentials, but the SDK still
        // requires a non-null provider before it will sign/send a request.
        return StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    }
}
