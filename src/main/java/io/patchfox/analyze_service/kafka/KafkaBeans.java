package io.patchfox.analyze_service.kafka;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.analyze_service.controllers.HealthCheckController;
import io.patchfox.analyze_service.controllers.RestInfoController;
import io.patchfox.analyze_service.controllers.TabulateController;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.analyze_service.services.RestInfoService;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaBeans {

    @Autowired
    private KafkaTemplate<String, ApiRequest> kafkaRequestTemplate;

    @Autowired
    private KafkaTemplate<String, ApiResponse> kafkaResponseTemplate;

    @Autowired 
    RestInfoService restInfoService;

    @Autowired
    ApplicationContext context;

    @Autowired
    EnvironmentComponent env;

    //
    // create topics for other services to send and receive messages on 
    //

    @Bean
    public NewTopic serviceRequestTopic() {
        return TopicBuilder.name(env.getKafkaRequestTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }

    @Bean
    public NewTopic serviceResponseTopic() {
        return TopicBuilder.name(env.getKafkaResponseTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }


    //
    // create listeners for the topics this service will send and receive on.
    // note that the reason we're not using the "env" component here is because the Kafka annotations are fun in that 
    // they don't allow for strings that aren't constants. You HAVE to use the property placeholder directly if you want
    // to make the id and topic configurable by way of the application.yml file.
    //

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.request.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.request-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiRequest"}
    )
    public void listenToRequestTopic(ApiRequest apiRequest) throws Exception {
        log.info("received apiRequest message: {}", apiRequest);
        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        var responseTopicName = apiRequest.getResponseTopicName();
        var txid = apiRequest.getTxid();
        var verb = apiRequest.getVerb();
        var resource = apiRequest.getUri();
        var resourceSignature = verb + "_" + resource.toString();
        try {
            var requestPair = new Pair<>(verb, resource);
            var handlerMethod = restInfoService.getHandlerFor(requestPair);
            var apiResponse = invokeMethod(txid, verb.toString(), resource.toString(), handlerMethod, now, apiRequest);
            apiResponse.setResponderName(env.getServiceName());
            apiResponse.setResponderResourceSignature(resourceSignature);
            kafkaResponseTemplate.send(responseTopicName, apiResponse);
        } catch (NullPointerException e) {
            var notFoundResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_NOT_FOUND)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, notFoundResponse);
        } catch (Exception e) {
            log.error("exception was: ", e);
            var serverErrorResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_INTERNAL_SERVER_ERROR)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, serverErrorResponse);
        }
    }

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.response.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.response-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiResponse"}
    )
    public void listenToResponseTopic(ApiResponse response) throws Exception {
        // here is where we inspect the response object and figure out what, if anything, we need to do next 
        log.info("received apiResponse message: {}", response);
    }


    //
    // helpers 
    //


    /**
     * 
     * @param topic
     * @param apiRequest
     */
    public void makeRequest(String topic, ApiRequest apiRequest) throws IllegalArgumentException {
        log.info("servicing apiRequest as Kafka message: {}", apiRequest);
        if ( !apiRequest.isValidForKafka() ) { 
            log.error("request obj failed validity check - rejecting and throwing exception");
            throw new IllegalArgumentException(); 
        }

        kafkaRequestTemplate.send(topic, apiRequest);
    }


    /**
     * helper to invoke the handler method we already know is associated with a given REST URI. The method allows us 
     * to invoke the method with the correct arguments w/o having to deal with a lot of wonky reflection that would 
     * otherwise be necessary. 
     * 
     * @param txid
     * @param verb
     * @param resource
     * @param handlerMethod
     * @param requestReceivedAt
     * @return
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     */
    private ApiResponse invokeMethod(
            UUID txid,
            String verb,
            String resource,
            HandlerMethod handlerMethod, 
            ZonedDateTime requestReceivedAt, 
            ApiRequest apiRequest
    ) throws IllegalAccessException, InvocationTargetException  {
        // this should never happen so long as we call the RestInfoService helper methods first to 
        // get a hold of the reflected Method obj representing the controller for the requested resource.
        // I don't like returning null so we assume we can't find what we're looking for until we find it
        // and replace this with whatever is more appropriate. 
        var rv = ApiResponse.builder()
                            .responderName(env.getServiceName())
                            .code(Response.SC_NOT_FOUND)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .build();
                            
        var restSignature = verb + "_" + resource;

        var bean = handlerMethod.getBean();
        // I think this is a name only until the object is actually created. It's typed as an "Object" in the 
        // HandlerMethod class.
        if (bean.getClass() == String.class) { bean = context.getBean((String)bean); }
        var beanMethod = handlerMethod.getMethod();
        log.debug("bean method name is: {}", beanMethod.getName());
        log.debug("bean var class is: {}", bean.getClass());
        log.debug("bean type is: {}", handlerMethod.getBeanType());
        log.debug("beanMethod is: {}", beanMethod);

        /*
         * 
         * WHEN YOU ADD A NEW REST CONTROLLER/SERVICE THIS IS WHERE YOU ADD THE HOOK TO ENSURE THE KAFKA LISTENER
         * KNOWS HOW TO INVOKE THE CONTROLLER METHOD
         * 
         */
        switch(restSignature) {
            case HealthCheckController.GET_PING_SIGNATURE:
            case RestInfoController.GET_REST_INFO_SIGNATURE:
                var re = (ResponseEntity<ApiResponse>)beanMethod.invoke(bean, txid, requestReceivedAt);
                rv = re.getBody();
                break;
            case TabulateController.POST_TABULATE_SIGNATURE:
                var queryStringParameters = (Map<String, String>)apiRequest.getQueryStringParameters();
                var datasetName = queryStringParameters.get("datasetName");
                var pageIndex = Integer.parseInt(queryStringParameters.get("pageIndex"));
                var pageSize = Integer.parseInt(queryStringParameters.get("pageSize"));
                var payload = (List<Long>)apiRequest.getData().get("datasourceEventIndexesByCommitDateAsc");
                re = (ResponseEntity<ApiResponse>)beanMethod.invoke(
                    bean, 
                    txid, 
                    requestReceivedAt, 
                    datasetName, 
                    pageIndex,
                    pageSize,
                    payload
                );
                rv = re.getBody();
                break;
        }
        return rv;
    }

}
