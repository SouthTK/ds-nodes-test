package com.example.coordinator.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.shared.model.LLMRequest;
import com.example.shared.model.UserRequest;

@Service
public class CoordinatorService {

    private final RestTemplate restTemplate;
    private final RequestStorage storage;
    

    public CoordinatorService(RestTemplate restTemplate, RequestStorage storage) {
        this.restTemplate = restTemplate;
        this.storage = storage;
    }

    public String search(LLMRequest request) {
        
        String id = UUID.randomUUID().toString(); 
        System.out.println("User Query: " + request.getUserQuery());

        UserRequest userRequest = new UserRequest();
        userRequest.setId(id);
        userRequest.setState("received");
        userRequest.setUserQuery(request.getUserQuery());

        storage.addRequest(id, userRequest);
        // save to queue

        // send object to other coordinator nodes

        // return id

        // HttpEntity<LLMRequest> entity = new HttpEntity<>(request);
        // try {

        //     ResponseEntity<String> response =
        //             restTemplate.exchange(
        //                     "http://localhost:8081/llm",
        //                     HttpMethod.POST,
        //                     entity,
        //                     new ParameterizedTypeReference<>() {}
        //             );

        //     // ingredientResults = response.getBody();
        // } catch (Exception e) {
        //     System.out.println("LLM Node unavailable");
        //     return null;
        // }
        return id;
    }
}