package io.patchfox.analyze_service.controllers;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.analyze_service.services.RestInfoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class RestInfoController {

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String REST_INFO_PATH = API_PATH_PREFIX + "/restinfo";
    public static final String GET_REST_INFO_SIGNATURE = "GET_" + REST_INFO_PATH;

    @Autowired
    RestInfoService restInfoService;

    @GetMapping(
        value = REST_INFO_PATH, 
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> restInfoHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt
    ) throws JsonProcessingException {
        var apiResponse = restInfoService.getRestInfo(txid, requestReceivedAt);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

}
