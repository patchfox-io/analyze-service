package io.patchfox.analyze_service;


import java.net.URI;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.analyze_service.controllers.RestInfoController;
import io.patchfox.analyze_service.helpers.RestHelper;
import io.patchfox.analyze_service.repositories.EditRepository;
import io.patchfox.analyze_service.repositories.FindingRepository;
import io.patchfox.package_utils.json.ApiRequest;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SpringBootApplication
@EntityScan("io.patchfox.db_entities.entities")
public class App {

    @Autowired 
    EnvironmentComponent env;

    @Autowired
    RestHelper restHelper;

    @Autowired
    EditRepository editRepository;

    @Autowired
    FindingRepository findingRepository;


	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

    //
    // this is how you send a message on the Kafka queue
    //
    // leave this uncommented - it not only smoke tests kafka on boot, it prints a list of available resource signatures 
    // to the log.
    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, ApiRequest> template) {

        findingRepository.createFindingsPerformanceIndexesNative();

        editRepository.creatEditIndexes();

        log.info("*!* SENDING TEST KAFKA MESSAGE *!*");

        var testMessage = ApiRequest.builder()
                                    .txid(UUID.randomUUID())
                                    .verb(ApiRequest.httpVerb.GET)
                                    .uri(URI.create(RestInfoController.REST_INFO_PATH))
                                    // setting ourselves as the intended receiver for response
                                    // in theory you can route the response to another service instance by setting this
                                    // to that service's response topic. 
                                    .responseTopicName(env.getKafkaResponseTopicName()) 
                                    .build();

        return args -> {
            template.send(env.getKafkaRequestTopicName(), testMessage);
        };
    }


    //
    // this is how you make a REST request
    //

    // @Bean
    // public ApplicationRunner runner1(RestHelper restHelper) {
    //     log.info("*!* SENDING TEST REST REQUEST *!*");

    //     return args -> {
    //         var testRequest = ApiRequest.builder()
    //                                     .txid(UUID.randomUUID())
    //                                     .verb(ApiRequest.httpVerb.GET)
    //                                     .uri(URI.create("http://worldtimeapi.org/api/timezone/America/Denver"))
    //                                     .responseTopicName(env.getServiceName())
    //                                     .build();

    //         try {
    //             var r = restHelper.makeRequest(testRequest);
    //             log.info("REST response is: {}", r);
    //         } catch (IllegalArgumentException e) {
    //             log.error("caught unexpected exception while attempting to make REST request", e);
    //         }
            
    //     };
    // }

}
