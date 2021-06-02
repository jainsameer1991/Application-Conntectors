package com.appconnectors;

import com.connector.hbase.HBaseModule;
import com.connector.hbase.HBaseResource;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;


public class ApplicationConnectorsApplication extends Application<ApplicationConnectorsConfiguration> {

    public static void main(String[] args) throws Exception {
        new ApplicationConnectorsApplication().run(args);
    }

    public void run(ApplicationConnectorsConfiguration applicationConnectorsConfiguration, Environment environment) {
        ApplicationConenctorsModule appModule = new ApplicationConenctorsModule(applicationConnectorsConfiguration);
        Injector injector = Guice.createInjector(appModule, new HBaseModule());
        environment.jersey().register(injector.getInstance(HBaseResource.class));
    }
}
