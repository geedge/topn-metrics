package com.galaxy.tsg.pojo;

import java.util.Map;

public class metricsEntity {
    private String name;
    private Map<String,String> tags;
    private Map<String,Long>  fields;
    private Long timestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, Long> getFields() {
        return fields;
    }

    public void setFields(Map<String, Long> fields) {
        this.fields = fields;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
