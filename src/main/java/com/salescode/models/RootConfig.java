package com.salescode.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Root configuration object that contains all entities
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RootConfig {

    @JsonProperty("entities")
    private List<EntityConfig> entities;
}