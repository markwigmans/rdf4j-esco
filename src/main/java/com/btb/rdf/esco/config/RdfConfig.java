package com.btb.rdf.esco.config;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RdfConfig {

    @Bean
    public Repository repository() {
        return new SailRepository(new MemoryStore());
    }
}
