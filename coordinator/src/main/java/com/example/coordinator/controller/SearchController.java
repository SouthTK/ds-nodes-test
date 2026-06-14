package com.example.coordinator.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.coordinator.service.CoordinatorService;
import com.example.coordinator.service.ProcessingService;
import com.example.coordinator.service.ConsensusService;
import com.example.coordinator.service.RequestStorage;

import com.example.coordinator.model.VoteRequest;
import com.example.coordinator.model.UserRequest;
import com.example.coordinator.model.NodesInfo;

import com.example.shared.model.LLMRequest;


@RestController
@RequestMapping("/")
public class SearchController {

    private final CoordinatorService coordinatorService;
    private final ConsensusService consensus;
    private final ProcessingService processingService;
    private final RequestStorage storage;

    public SearchController(CoordinatorService coordinatorService, ConsensusService consensus, 
            ProcessingService processingService, RequestStorage storage) {
        this.coordinatorService = coordinatorService;
        this.consensus = consensus;
        this.processingService = processingService;
        this.storage = storage;
    }
    // create request
    @PostMapping("/search")
    public String search(@RequestBody LLMRequest request) {
        return coordinatorService.search(request);
    }
    // get request's result
    @GetMapping("/get")
    public String get(@RequestParam String id) {
        return coordinatorService.get(id);
    } 
    // copy request to node
    @PostMapping("/copy") 
    public boolean copy(@RequestBody UserRequest request) { 
        storage.storeRequest(request.getId(), request);
        return true;
    }
    // ping node
    @PostMapping("/ping") 
    public boolean ping(@RequestParam String id, @RequestParam int term) { 
        return consensus.ping(id, term);
    }
    // ask for vote from node
    @PostMapping("/vote") 
    public boolean vote(@RequestBody VoteRequest request) { 
        return consensus.vote(request);
    }
    // join as coordinator with node's network
    @PostMapping("/join") 
    public NodesInfo join(@RequestParam String id) { 
        return consensus.join(id);
    }
    // join as llm node or recipe node with node's network
    @PostMapping("/apply") 
    public boolean apply(@RequestParam String id, @RequestParam String type) { 
        return consensus.apply(id, type);
    }
    // control node to join another node's network
    @PostMapping("/follow") 
    public boolean follow(@RequestParam String id) { 
        return consensus.follow(id);
    }


    @GetMapping("/gettest") // just for test TO DO: delete
    public UserRequest getTest(@RequestParam String id) {
        return coordinatorService.getTest(id);
    }
}