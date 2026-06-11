package com.example.coordinator.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.example.coordinator.model.UserRequest;

import com.example.shared.model.LLMRequest;

import java.util.concurrent.LinkedBlockingQueue;

import java.util.ArrayList;
import java.util.Random;
import java.util.List;

@Service
public class ProcessingService {

    private final RestTemplate restTemplate;
    private final RequestStorage storage;
    private boolean isLeader;

    private final List<String> llmNodes = new ArrayList<>();
    //private final List<String> dbNodes = new ArrayList<>();

    private final LinkedBlockingQueue<String> requestQueue;

    public ProcessingService(RestTemplate restTemplate, RequestStorage storage) {
        this.restTemplate = restTemplate;
        this.storage = storage;
        isLeader = false;

        this.requestQueue = new LinkedBlockingQueue<>(100);
    }

    public void setIsLeader(boolean input) {
        this.isLeader = input;
    }

    public boolean getIsLeader() {
        return this.isLeader;
    }

    public void apply() {
        // if nodes is llm, add to llmNodes
        // if nodes is db, add to dbNodes
        // if this node is leader, apply to other nodes too.
    }

    public void updateQueue() { 
        requestQueue.addAll(storage.getRequestList());
    } 

    public boolean addToQueue(String id) {
         try {
            requestQueue.put(id);
            return true;
        } catch (Exception e) {return false;}
    }

    public void processingThread() {
        // add time out??
        Thread checkingThread = new Thread(() -> {
            while (isLeader) { 
                try {
                    String id = requestQueue.take();
                    UserRequest request = storage.getRequest(id);

                    if (request.getState().equals("received")) {
                        LLMRequest llmRequest = new LLMRequest();
                        llmRequest.setUserQuery(request.getUserQuery());

                        // get the llm-node to process
                        // added returned result to request

                        Thread.sleep(5000); 
                        request.setState("formatted");
                        storage.storeRequest(id, request);
                        this.addToQueue(id);
                        storage.broadCastCopy(request);

                    } else if (request.getState().equals("formatted")) {

                        // get the db nodes to process
                        // added all the new result back to request

                        Thread.sleep(5000); 
                        request.setState("unformatted result");
                        storage.storeRequest(id, request);
                        this.addToQueue(id);
                        storage.broadCastCopy(request);

                    } else if (request.getState().equals("unformatted result")) {

                        // get the llm node to process
                        // added all the new result back to request

                        Thread.sleep(5000); 
                        request.setState("done");
                        request.setResult("final result is here");
                        storage.storeRequest(id, request);
                        storage.broadCastCopy(request);
                        
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

    private void sendToLLMNode(LLMRequest llmRequest) {
        int numberOfNodes = llmNodes.size();
        if (numberOfNodes >= 0)  {
            int attempt = 0;
            do {
                attempt = attempt + 1;
                String node = llmNodes.get(new Random().nextInt(numberOfNodes));
                try {
                    String targetUrl = "http://localhost:" + node + "/llm";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<LLMRequest> entity = new HttpEntity<>(llmRequest, headers);

                    // TO DO: fix below
                    //success = restTemplate.postForObject(targetUrl, entity, Boolean.class);
                    // if success, return result.
                } catch (Exception e) {System.out.println("Calling llm service failed.");}
            } while (attempt < 5);
        } else {System.out.println("No llm nodes found");}
    }
    
    // private void sendToDBNode() {}
}