package com.example.coordinator.service;

import jakarta.annotation.PostConstruct;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.shared.model.UserRequest;

@Service
public class ProcessingService {

    private final RestTemplate restTemplate;
    private final RequestStorage storage;
    private boolean isLeader;

    public ProcessingService(RestTemplate restTemplate, RequestStorage storage) {
        this.restTemplate = restTemplate;
        this.storage = storage;
        isLeader = true;
    }

    public void setIsLeader(boolean input) {this.isLeader = input;}

    // only leader run this 
    @PostConstruct
    public void processingThread() {
        Thread checkingThread = new Thread(() -> {
            while (isLeader) { 
                try {
                    String id = storage.getTask();
                    UserRequest request = storage.getRequest(id);

                    if (request.getState().equals("received")) {
                        // get the llm-node to process
                        // added returned result to request
                        Thread.sleep(5000); 
                        request.setState("formatted");
                        storage.addRequest(id, request);
                        // send the copy to other nodes

                    } else if (request.getState().equals("formatted")) {
                        // get the db nodes to process
                        // added all the new result back to request
                        Thread.sleep(5000); 
                        request.setState("unformatted result");
                        storage.addRequest(id, request);
                        // send the copy to other nodes

                    } else if (request.getState().equals("unformatted result")) {
                        // get the llm node to process
                        // added all the new result back to request
                        Thread.sleep(5000); 
                        request.setState("done");
                        request.setResult("final result is here");
                        storage.storeRequest(id, request);
                        // send the copy to other nodes
                        
                    } else {
                        System.out.println("Something went very wrong");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("The sleep was interrupted.");
                } catch (Exception e) {
                    System.out.println("The node cannot be called");
                }
            }
        });

        checkingThread.setDaemon(true); 
        checkingThread.start();
    }
}