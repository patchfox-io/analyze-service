package io.patchfox.analyze_service.interceptors;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.web.servlet.HandlerInterceptor;

import io.patchfox.package_utils.json.ApiRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;


/*
 * Ensures all requests to this service have a unique transaction id and a received_at timestamp associated with them.
 * The intention is this information to be generated here, at point of entry into the pipeline, and attached to every 
 * stage of the PatchFox pipeline.  
 */
@Slf4j
public class RequestEnrichmentInterceptor implements HandlerInterceptor {
    
    public static final String EVENT_RECEIVED_AT_ATTRIBUTE = "requestReceivedAt"; 

    @Override
    public boolean preHandle(
        HttpServletRequest request,
        HttpServletResponse response, 
        Object handler
    ) throws Exception {
        var txid = UUID.randomUUID();
        // we want to use the caller supplied one if possible in order to keep the same txid value
        // accross the entire pipeline workflow
        if (request.getHeader(ApiRequest.TXID_KEY) != null) {
            try {
                txid = UUID.fromString(request.getHeader(ApiRequest.TXID_KEY));
            } catch (IllegalArgumentException e) {
                log.warn(
                    "caller supplied invalid txid: {} -- using newly generated one",
                    request.getHeader(ApiRequest.TXID_KEY)
                );
            }
        }
        request.setAttribute(ApiRequest.TXID_KEY, txid);
        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        request.setAttribute(EVENT_RECEIVED_AT_ATTRIBUTE, now);
        return true;
    }

}
