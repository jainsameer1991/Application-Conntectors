package com.appconnectors;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;

public class ApplicationConenctorsModule extends AbstractModule {

    private final ApplicationConnectorsConfiguration configuration;

    @Inject
    public ApplicationConenctorsModule(ApplicationConnectorsConfiguration configuration) {
        this.configuration = configuration;
    }

    protected void configure() {

    }
}
