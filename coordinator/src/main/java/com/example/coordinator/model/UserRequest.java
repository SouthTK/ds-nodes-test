package com.example.coordinator.model;

import com.example.shared.model.RecipeQuery;
import com.example.shared.model.RecipeQueryResult;
import com.example.shared.model.StatusState;

import java.util.List;

public class UserRequest {
    private String id;
    private String state;
    private String userQuery;
    private RecipeQuery recipeQuery;
    private RecipeQueryResult recipeQueryResult;
    private String result;

    public UserRequest() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getUserQuery() { return userQuery; }
    public void setUserQuery(String userQuery) { this.userQuery = userQuery; }

    public RecipeQuery getRecipeQuery() { return recipeQuery; }
    public void setRecipeQuery(RecipeQuery recipeQuery) { this.recipeQuery = recipeQuery; }

    public RecipeQueryResult getRecipeQueryResult() { return recipeQueryResult; }
    public void setRecipeQueryResult(RecipeQueryResult recipeQueryResult) { this.recipeQueryResult = recipeQueryResult; }

    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }
}