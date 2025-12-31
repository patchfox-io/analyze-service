package io.patchfox.analyze_service;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import io.patchfox.analyze_service.interceptors.ErrorViewInterceptor;
import io.patchfox.analyze_service.interceptors.RequestEnrichmentInterceptor;


@Configuration
@EnableWebMvc
public class WebConfig implements WebMvcConfigurer {

	@Override
	public void addInterceptors(InterceptorRegistry registry) {
		registry.addInterceptor(new RequestEnrichmentInterceptor());
		registry.addInterceptor(new ErrorViewInterceptor()).addPathPatterns("/error");
	}
}
