package com.microservices.elasticsearch.dynamic.query.config;

import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI elasticsearchReactiveOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Elasticsearch Reactive Service API")
                        .description("Spring Boot 3.x Reactive Elasticsearch service")
                        .version("1.0.0")
                        .contact(new Contact().name("API Support").email(""))
                        .license(new License().name("Apache 2.0").url("https://www.apache.org/licenses/LICENSE-2.0"))
                );
    }

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                .group("public")
                .pathsToMatch("/api/**")
                .build();
    }

    // Add a header parameter to Elasticsearch API endpoints in the OpenAPI doc
    // Appears in Swagger UI for operations under /api/elasticsearch/**
    @Bean
    public OpenApiCustomizer elasticsearchHeaderOpenApiCustomizer() {
        final String headerName = "X-Client-Id"; // change to your desired header name
        final String headerDescription = "Optional client identifier header";

        return openApi -> {
            if (openApi.getPaths() == null) return;
            openApi.getPaths().forEach((path, pathItem) -> {
                if (path == null || !path.startsWith("/api/elasticsearch/")) return;
                pathItem.readOperations().forEach(operation -> {
                    boolean alreadyPresent = operation.getParameters() != null &&
                            operation.getParameters().stream().anyMatch(p -> headerName.equalsIgnoreCase(p.getName()));
                    if (!alreadyPresent) {
                        Parameter headerParam = new Parameter()
                                .in("header")
                                .name(headerName)
                                .required(false)
                                .description(headerDescription)
                                .schema(new StringSchema());
                        operation.addParametersItem(headerParam);
                    }
                });
            });
        };
    }
}
