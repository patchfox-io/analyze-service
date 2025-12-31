package io.patchfox.analyze_service.services;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RestInfoService {
    private final RequestMappingHandlerMapping handlerMapping;

    @Autowired
    EnvironmentComponent env;

    @Autowired
    private Jackson2ObjectMapperBuilder mapperBuilder;

    @Autowired
    public RestInfoService(RequestMappingHandlerMapping handlerMapping) {
        this.handlerMapping = handlerMapping;
    }

    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @return
     * @throws JsonProcessingException 
     */
    public ApiResponse getRestInfo(UUID txid, ZonedDateTime requestReceivedAt) throws JsonProcessingException {
        // constructor populates some of the other fields in the response obj
        var rv = ApiResponse.builder()
                            .code(Response.SC_OK)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt)
                            .data(getRestInfo())
                            .build();
        return rv;
    }
    
    /**
     * returns a map <HTTP_VERB, List<REST_PATH>> containing all the REST paths registered with this service. 
     * 
     * @return
     * @throws JsonProcessingException 
     */
    public Map<String, Object> getRestInfo() throws JsonProcessingException { 
        Map<String, List<String>> rv_tmp = new HashMap<>();

        for (var key : handlerMapping.getHandlerMethods().keySet()) {
            var method = key.getMethodsCondition().toString();
            // because we get something like "[GET]" and we want just "GET"
            method = trimMethod(method);
            // now let's make extra sure we've parsed it correctly 
            var httpMethod = HttpMethod.valueOf(method);
            // to catch the internal /error mapping that has no verb associated with it
            if (httpMethod == null || httpMethod.toString().isEmpty()) { continue; }
            var pathSet = key.getPatternValues();
            if (rv_tmp.containsKey(method)) {
                rv_tmp.get(method).addAll(pathSet.stream().toList());
            } else {
                rv_tmp.put(method, pathSet.stream().collect(Collectors.toList()));
            }                                
        }

        Map<String, Object> rv = new HashMap<>();
        var mapper = mapperBuilder.build();
        for (var entry : rv_tmp.entrySet()) {
            rv.put(
                entry.getKey(), 
                entry.getValue()
                //mapper.writeValueAsString(entry.getValue())
                // entry.getValue()
                //      .stream()
                //      .map(Object::toString)
                //      .collect(Collectors.joining(",")
                //)
            );
        }

        return rv;
    }

    /**
     * 
     * @param requestPair
     * @return
     */
    public HandlerMethod getHandlerFor(Pair<ApiRequest.httpVerb, URI> requestPair) throws NullPointerException {
        HandlerMethod rv = null;
        log.debug("method is: {}", requestPair.getLeft());
        log.debug("signature is: {}", requestPair.getRight());

        for (var entry : handlerMapping.getHandlerMethods().entrySet()) {
            var method = entry.getKey().getMethodsCondition().toString();
            // because we get something like "[GET]" and we want just "GET"
            method = trimMethod(method);
            var httpMethod = HttpMethod.valueOf(method).toString();
            log.debug("httpmethod is: {}", httpMethod);
            log.debug("patternvalues is: {}", entry.getKey().getPatternValues());
            if ( !requestPair.getLeft().toString().equals(httpMethod) ) { continue; }
            if ( !entry.getKey().getPatternValues().contains(requestPair.getRight().toString()) ) { continue; }
            rv = entry.getValue();
        }    

        if (rv == null) { throw new NullPointerException(); }
        return rv;
    }

    /**
     * 
     * @param method
     * @return
     */
    public String trimMethod(String method) {
        if (method.charAt(0) == '[') {
            var methodSize = method.length();
            return method.substring(1, methodSize -1);
        }
        return method;
    }
}
