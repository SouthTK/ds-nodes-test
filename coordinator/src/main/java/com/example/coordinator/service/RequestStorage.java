package com.example.coordinator.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.List;
import java.util.HashMap;

import com.example.shared.model.UserRequest;

@Component // Declares this as a shared Singleton bean
public class RequestStorage {

    private final LinkedBlockingQueue<String> requestQueue = new LinkedBlockingQueue<>(100);
    private final HashMap<String, UserRequest> requestStatus = new HashMap<>();

    // not thread safe yet
    public void addRequest(String id, UserRequest request) {
        try {
            requestStatus.put(id, request);
            System.out.println("added");
            if (!request.getState().equals("done")) {
                requestQueue.put(id);
            }
            
        } catch (Exception e) {}

    }
    // not thread safe yet
    public UserRequest getRequest(String id) {
        return requestStatus.get(id);
    }

    public String getTask() {
        try {
            return requestQueue.take();
        } catch (Exception e) {return "error";}
        
    }
}