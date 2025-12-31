package io.patchfox.analyze_service.services;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.package_utils.json.ApiResponse;

@Component
public class HealthCheckService {

    @Autowired
    EnvironmentComponent env;

    public ApiResponse getHealthCheckResponse(UUID txid, ZonedDateTime requestReceivedAt) {
        // constructor populates some of the other fields in the response obj
        var rv = ApiResponse.builder()
                            .code(Response.SC_OK)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .data(Map.of("response", "pong"))
                            .build();
                            
        return rv;
    }

}
