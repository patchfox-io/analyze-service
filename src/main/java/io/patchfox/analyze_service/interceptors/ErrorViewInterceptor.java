package io.patchfox.analyze_service.interceptors;


import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class ErrorViewInterceptor implements HandlerInterceptor {
    

    @Override
    public boolean preHandle(
        HttpServletRequest request,
        HttpServletResponse response, 
        Object handler
    ) throws Exception {
        throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
    }
}
