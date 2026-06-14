package com.example.coordinator.service;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;

import java.util.concurrent.ThreadLocalRandom;

import com.example.coordinator.model.VoteRequest;
import com.example.coordinator.model.UserRequest;
import com.example.coordinator.model.NodesInfo;
import com.example.shared.model.LLMRequest;

@Component 
public class ConsensusService {
    private final RestTemplate restTemplate;
    private final ProcessingService processingService;
    private final RequestStorage storage;

    private final HashSet<String> nodesList = new HashSet<>();
    @Value("${server.port:8080}")
    private String nodeId; 
    private boolean voted;
    
    private String nodeStatus; // do I need to sync???
    private String leaderId; // do I need to sync???
    private boolean leaderAlive;
    private int term;

    public ConsensusService(RestTemplate restTemplate, 
            ProcessingService processingService, RequestStorage storage) {
        this.restTemplate = restTemplate;
        this.processingService = processingService;
        this.storage = storage;

        this.voted = false;
        this.nodeStatus = "follower";
        this.leaderId = null;
        this.term = 0;
        this.leaderAlive = false;
        }

    public boolean vote(VoteRequest request) { 
        int requestCount = storage.getRequestList().size();

        if (this.term <= request.getTerm() && requestCount <= request.getRequestCount()) {
            if(this.term < request.getTerm()) {
                this.term = request.getTerm();
                this.voted = false;
            }

            int candidateRequestCount = request.getRequestCount();
            if (candidateRequestCount >= requestCount && !this.voted) {
                this.voted = true;
                this.leaderId = request.getCandidateId();
                this.leaderAlive = true;
                this.nodeStatus = "follower";
                processingService.setIsLeader(false);
                return true;
                } 
            }
        return false;
    }

    public boolean ping(String id, int term) {
        if (term >= this.term) {
            this.leaderId = id;
            this.leaderAlive = true;
            this.nodeStatus = "follower";
            processingService.setIsLeader(false);
            this.term = term;
            return true;
        } else {return false;}
    }

    public NodesInfo join(String id) {
        if (nodeStatus.equals("leader")) {
            Boolean validity = false;
            try {
                String targetUrl = "http://localhost:" + id + "/ping";
                String urlTemplate = UriComponentsBuilder.fromHttpUrl(targetUrl)
                        .queryParam("id", nodeId)
                        .queryParam("term", this.term)
                        .encode()
                        .toUriString();

                validity = restTemplate.postForObject(urlTemplate, null, Boolean.class);
            } catch (Exception e) {System.out.println("Ping failed.");}

            if (validity) {
                nodesList.add(id);
                storage.addNode(id);

                for (String node : nodesList) {
                    try {
                    String targetUrl = "http://localhost:" + node + "/join";
                    String urlTemplate = UriComponentsBuilder.fromHttpUrl(targetUrl)
                            .queryParam("id", id)
                            .encode()
                            .toUriString();
                    restTemplate.postForObject(urlTemplate, null, Boolean.class);
                    } catch (Exception e) {System.out.println("Ping failed.");}
                }
                List coordinators = new ArrayList<>(nodesList);
                coordinators.add(nodeId);
                coordinators.remove(id);

                NodesInfo info = new NodesInfo();
                info.setCoordinatorNodes(coordinators);
                info.setLlmNodes(processingService.getLlmNodes());
                info.setRecipeNodes(processingService.getDbNodes());
                return info;
            }
            return null;
        
        } else {
            nodesList.add(id);
            storage.addNode(id);
        }
        return null;
    }

    public boolean follow(String id) {
        if (id.equals(nodeId)) {return false;}
        try {
            String targetUrl = "http://localhost:" + id + "/join";
            String urlTemplate = UriComponentsBuilder.fromHttpUrl(targetUrl)
                    .queryParam("id", nodeId)
                    .encode()
                    .toUriString();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Void> entity = new HttpEntity<>(headers);
            NodesInfo info = restTemplate.postForObject(urlTemplate, entity, NodesInfo.class);
            
            if (info == null) {
                return false;
            } else {
                nodesList.clear();
                nodesList.addAll(info.getCoordinatorNodes());
                storage.setNode(info.getCoordinatorNodes());
                processingService.setLlmNodes(info.getLlmNodes());
                processingService.setDbNodes(info.getRecipeNodes());
                return true;
            }
        } catch (Exception e) {
            System.out.println("Following nodes failed.");
            return false;
        }
    }

    public boolean apply(String id, String type) {
        if (nodeStatus.equals("leader")) {
            // try service first maybe
            processingService.apply(id, type);
            for (String node : nodesList) {
                try {
                String targetUrl = "http://localhost:" + node + "/apply";
                String urlTemplate = UriComponentsBuilder.fromHttpUrl(targetUrl)
                        .queryParam("id", id)
                        .queryParam("type", type)
                        .encode()
                        .toUriString();
                boolean result = restTemplate.postForObject(urlTemplate, null, Boolean.class);
                if (!result) return result;
                } catch (Exception e) {System.out.println("Broadcasting new worker nodes failed.");}
            }
            return true;
        } else {
            processingService.apply(id, type);
        }
        return false;
    }

    public String redirect(LLMRequest request) {
        System.out.println("Redirecting..");
        if (leaderId != null) {
            try {
                String targetUrl = "http://localhost:" + leaderId + "/search";
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<LLMRequest> entity = new HttpEntity<>(request, headers);
                return restTemplate.postForObject(targetUrl, entity, String.class);
            } catch (Exception e) {System.out.println("Redirecting fails.");}
        }
        return null;
    }

    @Scheduled(fixedDelay = 1000)
    public void pingingThread() {
        if (nodeStatus.equals("leader")) {
            for (String node : nodesList) {
                try {
                    String targetUrl = "http://localhost:" + node + "/ping";
                    String urlTemplate = UriComponentsBuilder.fromHttpUrl(targetUrl)
                            .queryParam("id", nodeId)
                            .queryParam("term", this.term)
                            .encode()
                            .toUriString();

                    Boolean response = restTemplate.postForObject(urlTemplate, null, Boolean.class);
                    if (!response) {
                        nodeStatus = "follower"; 
                        processingService.setIsLeader(false);
                        }
                } catch (Exception e) {System.out.println("Broadcast failed.");}
            }
            System.out.println("Pinging other nodes.");
        }
    }

    @Scheduled(fixedDelay = 5000)
    public void scheduledTask() {
        System.out.println("Current Status: " + this.nodeStatus + " " + this.term);
        System.out.println(nodesList);

        if (this.nodeStatus == "follower") {
            if (this.leaderAlive) {this.leaderAlive = false;}
            else {
                this.nodeStatus = "candidate";
                this.leaderId = null;
                processingService.setIsLeader(false);
                while (this.nodeStatus.equals("candidate")) {
                    try {
                        long randomDelay = ThreadLocalRandom.current().nextLong(1000, 2000 + 1);
                        Thread.sleep(randomDelay);
                    } catch (Exception e) {}

                    if (!this.nodeStatus.equals("candidate")) {return;}

                    this.term = term + 1;
                    this.voted = false;
                    int vote = 1;

                    VoteRequest request = new VoteRequest();
                    request.setRequestCount(storage.getRequestList().size());
                    request.setCandidateId(this.nodeId);
                    request.setRequestCount(4);
                    request.setTerm(this.term);

                    System.out.println("Start election " + this.term);
                    for (String node : nodesList) {
                        try {
                            String targetUrl = "http://localhost:" + node + "/vote";
                            HttpHeaders headers = new HttpHeaders();
                            headers.setContentType(MediaType.APPLICATION_JSON);
                            HttpEntity<VoteRequest> entity = new HttpEntity<>(request, headers);
                            Boolean result = restTemplate.postForObject(targetUrl, entity, Boolean.class);
                            if (result) {vote = vote + 1;}
                        } catch (Exception e) {System.out.println("Ask for vote failed.");}
                    }

                    if (vote > ((nodesList.size() + 1) / 2) && this.nodeStatus.equals("candidate")) {
                        this.nodeStatus = "leader";
                        this.leaderId = null;
                        processingService.setIsLeader(true);
                        processingService.processingThread();
                        processingService.updateQueue();
                        System.out.println("Won");
                    }
                    else {System.out.println("Lost");}
                }
            }
        }
    }
}