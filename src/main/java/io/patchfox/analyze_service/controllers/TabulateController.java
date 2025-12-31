package io.patchfox.analyze_service.controllers;


import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.packageurl.MalformedPackageURLException;

import io.patchfox.analyze_service.services.TabulateService;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class TabulateController {

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String TABULATE_PATH = API_PATH_PREFIX + "/tabulate";
    public static final String POST_TABULATE_SIGNATURE = "POST_" + TABULATE_PATH;

    @Autowired
    TabulateService tabulateService;

    @PostMapping(
        value = TABULATE_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> tabulateHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam String datasetName,
        @RequestParam Integer pageIndex,
        @RequestParam Integer pageSize,
        @RequestBody List<Integer> datasourceEventIndexesByCommitDateAsc
    ) throws MalformedPackageURLException {
        
        var apiResponse = ApiResponse.builder()
                                     .txid(txid)
                                     .requestReceivedAt(requestReceivedAt)
                                     .code(HttpStatus.BAD_REQUEST.value())
                                     .serverMessage("datasourceEventIndexesByCommitDateAsc can not be empty")
                                     .build();

        if ( !datasourceEventIndexesByCommitDateAsc.isEmpty() ) {
            apiResponse = tabulateService.tabulate(
                txid, 
                requestReceivedAt, 
                datasetName, 
                pageIndex,
                pageSize,
                datasourceEventIndexesByCommitDateAsc
            );
        }
        
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

}
