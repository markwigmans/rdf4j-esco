package com.btb.rdf.esco.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class ApplicationConfig {

    @Value("${chunk.size:100}")
    private int chunkSize;

    @Value("${max.count:0x7fffffff}")
    private int maxCount;

    @Value("${delete.all:true}")
    private boolean delete;

    @Value("${pool.size:8}")
    private int poolSize;
}
