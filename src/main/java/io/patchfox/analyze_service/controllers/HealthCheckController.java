package io.patchfox.analyze_service.controllers;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RestController;

import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.analyze_service.services.HealthCheckService;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class HealthCheckController {
    
    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String PING_PATH = API_PATH_PREFIX + "/ping";
    public static final String GET_PING_SIGNATURE = "GET_" + PING_PATH;

    @Autowired
    HealthCheckService healthCheckService;

    @GetMapping(
        value = PING_PATH, 
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> healthCheckHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt
    ) {
        var apiResponse = healthCheckService.getHealthCheckResponse(txid, requestReceivedAt);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }    

}
