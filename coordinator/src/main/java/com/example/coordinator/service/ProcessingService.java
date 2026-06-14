package com.example.coordinator.service;

import org.springframework.stereotype.Service;

import org.springframework.core.ParameterizedTypeReference;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.example.coordinator.model.UserRequest;

import com.example.shared.model.LLMRequest;
import com.example.shared.model.RecipeQuery;
import com.example.shared.model.RecipeQueryResult;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

@Service
public class ProcessingService {

    private final RestTemplate restTemplate;
    private final RequestStorage storage;
    private boolean isLeader;

    private final HashSet<String> llmNodes = new HashSet<>();
    private final HashSet<String> dbNodes = new HashSet<>();

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

    public List<String> getLlmNodes() {
        return new ArrayList<>(llmNodes);
    }

    public void setLlmNodes(List<String> list) {
        this.llmNodes.clear();
        this.llmNodes.addAll(list);
    }

    public List<String> getDbNodes() {
        return new ArrayList<>(dbNodes);
    }

    public void setDbNodes(List<String> list) {
        this.dbNodes.clear();
        this.dbNodes.addAll(list);
    }

    public boolean apply(String id, String type) {
        if (type.equals("llm")) {
            llmNodes.add(id);
        } else if (type.equals("db")) {
            dbNodes.add(id);
        }
        return true;
    }

    public void updateQueue() { 
        requestQueue.addAll(storage.getRequestList());
    } 

    public boolean addToQueue(String id) {
        return requestQueue.offer(id);
    }

    public boolean putToQueue(String id) {
        try {
            requestQueue.put(id);
            return true;
        } catch (Exception e) {return false;}
    }

    public void processingThread() {
        Thread checkingThread = new Thread(() -> {
            while (isLeader) { 
                try {
                    String id = requestQueue.poll(1, TimeUnit.SECONDS); 
                    if (id == null) {continue;}
                    UserRequest request = storage.getRequest(id);
                    if (request == null) {continue;}

                    if (request.getState().equals("received")) {
                        LLMRequest llmRequest = new LLMRequest();
                        llmRequest.setUserQuery(request.getUserQuery());
                        RecipeQuery result = sendToLLMNode(llmRequest);

                        if (result != null) {
                            request.setState("formatted");
                            request.setRecipeQuery(result);
                            storage.storeRequest(id, request);
                            this.putToQueue(id);
                            storage.broadCastCopy(request);
                        } else {
                            System.out.println("No result, format request failed.");
                            this.putToQueue(id);
                        }

                    } else if (request.getState().equals("formatted")) {
                        RecipeQuery recipeQuery = request.getRecipeQuery();
                        List<RecipeQueryResult> result = sendToDBNode(recipeQuery);

                        if (result != null && !result.isEmpty()) {
                            request.setState("unformatted results");
                            request.setRecipeQueryResults(result);
                            storage.storeRequest(id, request);
                            this.putToQueue(id);
                            storage.broadCastCopy(request);
                        } else {
                            System.out.println("No result, request failed.");
                            this.putToQueue(id);
                        }

                    } else if (request.getState().equals("unformatted results")) {

                        String finalResult = sendToLLMAnswerNode(request);
                        request.setState("done");
                        request.setResult(finalResult);
                        storage.storeRequest(id, request);
                        storage.broadCastCopy(request);
                        
                    } else {
                        System.out.println("Something went very wrong");
                        this.putToQueue(id);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("The sleep was interrupted.");
                } catch (Exception e) {
                    System.out.println("The node cannot be called");
                    e.printStackTrace();
                }
            }
        });

        checkingThread.setDaemon(true); 
        checkingThread.start();
    }

    private RecipeQuery sendToLLMNode(LLMRequest llmRequest) {
        int numberOfNodes = llmNodes.size();
        if (numberOfNodes > 0)  {
            int attempt = 0;
            do {
                attempt = attempt + 1;
                String node = (String) llmNodes.toArray()[ThreadLocalRandom.current().nextInt(numberOfNodes)];
                try {
                    String targetUrl = "http://localhost:" + node + "/llm/decompose";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<LLMRequest> entity = new HttpEntity<>(llmRequest, headers);

                    return restTemplate.postForObject(targetUrl, entity, RecipeQuery.class);
                } catch (Exception e) {System.out.println("Calling llm service failed.");}
            } while (attempt < 5);
            return null;
        } else {
            System.out.println("No llm nodes found");
            return null;
        }
    }

    private String sendToLLMAnswerNode(UserRequest userRequest) {
        int numberOfNodes = llmNodes.size();
        if (numberOfNodes > 0)  {
            int attempt = 0;
            do {
                attempt = attempt + 1;
                String node = (String) llmNodes.toArray()[ThreadLocalRandom.current().nextInt(numberOfNodes)];
                try {
                    String targetUrl = "http://localhost:" + node + "/llm/answer";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<UserRequest> entity = new HttpEntity<>(userRequest, headers);

                    return restTemplate.postForObject(targetUrl, entity, String.class);
                } catch (Exception e) {
                    System.out.println("Calling llm service failed.");
                    e.printStackTrace();
                }
            } while (attempt < 5);
            return null;
        } else {
            System.out.println("No llm nodes found");
            return null;
        }
    }
    
    private List<RecipeQueryResult> sendToDBNode(RecipeQuery recipeQuery) {
        int numberOfNodes = dbNodes.size();
        List<RecipeQueryResult> results = new ArrayList<>();
        if (numberOfNodes >= 0)  {
            for (String node : dbNodes) {
                try {
                    String targetUrl = "http://localhost:" + node + "/recipes/search";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<RecipeQuery> entity = new HttpEntity<>(recipeQuery, headers);

                    ResponseEntity<List<RecipeQueryResult>> response = restTemplate.exchange(
                            targetUrl, HttpMethod.POST, entity,
                            new ParameterizedTypeReference<List<RecipeQueryResult>>() {}
                    );

                    if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                        results.addAll(response.getBody());
                    }
                } catch (Exception e) {System.out.println("Calling recipe node (" + node + ") service failed.");}
            }

            results.sort((r1, r2) -> {
                Double s1 = r1.getScore() != null ? r1.getScore() : 0.0;
                Double s2 = r2.getScore() != null ? r2.getScore() : 0.0;
                return Double.compare(s2, s1);
            });

            if (results.size() > 10) {
                return new ArrayList<>(results.subList(0, 10));
            } else {return results;}
        } else {
            System.out.println("No recipe nodes found");
            return null;
        }
    }
}