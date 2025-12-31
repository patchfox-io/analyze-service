package io.patchfox.analyze_service.advice;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import io.patchfox.analyze_service.interceptors.RequestEnrichmentInterceptor;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;

/*
 * exception interceptors to ensure all exceptions are in standard format and have txids attached
 */
@ControllerAdvice(annotations = RestController.class)
public class ExceptionControllerAdvice extends ResponseEntityExceptionHandler {

    private ApiResponse getApiResponseWithRequestEnrichments(HttpStatusCode httpStatus, WebRequest request) {
        var txidKey = ApiRequest.TXID_KEY;
        var txid = (UUID)request.getAttribute(txidKey, RequestAttributes.SCOPE_REQUEST);
        var eventReceivedAtKey = RequestEnrichmentInterceptor.EVENT_RECEIVED_AT_ATTRIBUTE;
        var eventReceivedAt = (ZonedDateTime)request.getAttribute(eventReceivedAtKey, RequestAttributes.SCOPE_REQUEST);
        
        return ApiResponse.builder()
                          .code(httpStatus.value())
                          .txid(txid)
                          .requestReceivedAt(eventReceivedAt.toString())
                          .build();
    }


    /*
     * anything in the app that throws a spring http exception we handle here 
     */
    @ExceptionHandler({HttpStatusCodeException.class})
    public ResponseEntity<ApiResponse> handleHttpException(HttpStatusCodeException ex, WebRequest request) {
        var httpStatusCode = ex.getStatusCode();
        var apiResponse = getApiResponseWithRequestEnrichments(httpStatusCode, request);
        return new ResponseEntity<>(apiResponse, httpStatusCode);
    }


    /*
     * this shouldn't happen but if it does we'll capture it here and report it as a http 500 
     */
    @ExceptionHandler({Exception.class})
    public ResponseEntity<ApiResponse> handleGeneralException(Exception ex, WebRequest request) {
        logger.error("caught unexpected general exception: ", ex);
        var httpStatusCode = HttpStatus.INTERNAL_SERVER_ERROR;
        var apiResponse = getApiResponseWithRequestEnrichments(httpStatusCode, request);
        return new ResponseEntity<>(apiResponse, httpStatusCode);
    }

}
