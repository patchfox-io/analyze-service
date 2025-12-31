package io.patchfox.analyze_service.controllers;


import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.servlet.error.AbstractErrorController;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.patchfox.analyze_service.components.EnvironmentComponent;
import io.patchfox.package_utils.json.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;


/*
 * overrides default spring mvc /error page to be an app-standard json response and not an html one  
 */
@RequestMapping({"${server.error.path:${error.path:/error}}"})
@RestController
@Slf4j
public class BasicErrorControllerOverride extends AbstractErrorController {

    @Autowired
    EnvironmentComponent env;

    public BasicErrorControllerOverride(ErrorAttributes errorAttributes) {
      super(errorAttributes);
    }

    @RequestMapping
    public ResponseEntity<ApiResponse> error(
          HttpServletRequest request, 
          @RequestAttribute UUID txid, 
          @RequestAttribute ZonedDateTime requestReceivedAt
    ) {
      HttpStatus status = this.getStatus(request);

      var apiResponse = ApiResponse.builder()
                                   .responderName(env.getServiceName())
                                   .code(status.value())
                                   .txid(txid)
                                   .requestReceivedAt(requestReceivedAt.toString())
                                   .build();
      
      return new ResponseEntity<>(apiResponse, status);
    }


}
