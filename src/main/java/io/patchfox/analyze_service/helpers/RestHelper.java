package io.patchfox.analyze_service.helpers;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.analyze_service.controllers.RestInfoController;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class RestHelper {

    private final RestClient restClient; 

    @Autowired
    EnvironmentComponent env;

    public RestHelper(RestClient.Builder restClientBuilder) {
        // here is where we can inject default behavior such as baseUrl, default headers, etc. 
        this.restClient = restClientBuilder.build();
    }

    /**
     * convenience method to encourage devs to use the spring tooling to construct URIs
     * @return
     */
    public UriComponentsBuilder getUriComponentsBuilder() { return UriComponentsBuilder.newInstance(); }


    /**
     * method for making http requests and mapping responses to an ApiResponse object. handles redirects as well as 
     * 4xx and 5xx responses. 
     * 
     * @param apiRequest 
     * @return
     * @throws IllegalArgumentException if ApiRequest.isValid() for argument reports false. 
     */
    public ApiResponse makeRequest(ApiRequest apiRequest) throws IllegalArgumentException {
        log.info("servicing apiRequest as REST call: {}", apiRequest);

        if ( !apiRequest.isValidForRest() ) { 
            log.error("request obj failed validity check - rejecting and throwing exception");
            throw new IllegalArgumentException(); 
        }

        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        var verb = apiRequest.getVerb();
        try {
            String rBody = this.restClient.method(HttpMethod.valueOf(verb.toString()))                               
                               .uri(apiRequest.getUriWithQueryStringParameters())
                               .headers( headers -> { 
                                    for (var e : apiRequest.getHeaders().entrySet()) { 
                                        headers.add(e.getKey(), e.getValue());
                                     }
                                })
                               .retrieve()
                               // if you have a json class that can catch the json response put that here 
                               // instead of String.class
                               .body(String.class); 
    
            // we're assuming the response was 200 if an error didn't get thrown 
            // again - desired behavior is to map the response to an appropriate json pojo. don't leave this as a string
            return ApiResponse.builder()
                              .responderName(env.getServiceName())
                              .code(Response.SC_OK)
                              .txid(apiRequest.getTxid())
                              .requestReceivedAt(now.toString())
                              .data(Map.of("response", rBody))
                              .build();

        } catch (RestClientResponseException e) {
            log.warn("caught http response error making request: {}", e.getStatusText());
            HttpStatus status = HttpStatus.valueOf(e.getStatusCode().value());
            return ApiResponse.builder()
                              .responderName(env.getServiceName())
                              .code(status.value())
                              .txid(apiRequest.getTxid())
                              .requestReceivedAt(now.toString())
                              .build();
        }

    }

}
