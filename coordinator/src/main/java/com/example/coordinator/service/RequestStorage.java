package com.example.coordinator.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;


import com.example.shared.model.UserRequest;

@Component // Declares this as a shared Singleton bean
public class RequestStorage {

    private final LinkedBlockingQueue<String> requestQueue = new LinkedBlockingQueue<>(100);
    private final ConcurrentHashMap<String, UserRequest> requestStatus = new ConcurrentHashMap<>();

    public void addRequest(String id, UserRequest request) {
        try {
            requestStatus.put(id, request);
            if (!request.getState().equals("done")) {
                requestQueue.put(id);
            }
        } catch (Exception e) {}
    }

    public void storeRequest(String id, UserRequest request) {
        try {
            requestStatus.put(id, request);
        } catch (Exception e) {}
    }

    public UserRequest getRequest(String id) {
        return requestStatus.get(id);
    }

    public String getTask() {
        try {
            return requestQueue.take();
        } catch (Exception e) {return "error";}
    }

    public void broadCastCopy(UserRequest request) {
        // try {
        //     String url = "http://localhost:8090/copy";

        //     HttpHeaders headers = new HttpHeaders();
        //     headers.setContentType(MediaType.APPLICATION_JSON);

        //     HttpEntity<UserRequest> requestEntity = new HttpEntity<>(request, headers);
        //     Boolean success = restTemplate.postForObject(url, requestEntity, Boolean.class);
        //     System.out.println("Copy sent");
        // } catch (Exception e) {System.out.println("Broadcast failed.")}
    }

    public void updateQueue() {} // for new leader mid runtime
}