package io.patchfox.analyze_service.components;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutorConfig {

    @Bean(name = "executor")
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(10); 
    }
}

