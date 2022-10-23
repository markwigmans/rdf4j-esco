package com.btb.rdf.esco.batch;

import com.btb.rdf.esco.config.ApplicationConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ProcessTransversals {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;

    @Bean("ProcessTransversals.step")
    public Step step() {
        return this.stepBuilderFactory.get("Transversals")
                .transactionManager(tm)
                .<TransversalInput, TransversalInput>chunk(config.getChunkSize())
                .reader(transversalInputMultiResourceItemReader())
                .writer(transversalInputItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public MultiResourceItemReader<TransversalInput> transversalInputMultiResourceItemReader() {
        Resource[] inputFiles = {
                new ClassPathResource("transversalSkillsCollection_nl.csv"),
                new ClassPathResource("ictSkillsCollection_nl.csv"),
                new ClassPathResource("languageSkillsCollection_nl.csv")
        };

        return new MultiResourceItemReaderBuilder<TransversalInput>()
                .name("multiCustomerReader")
                .resources(inputFiles)
                .delegate(transversalInputFlatFileItemReader())
                .build();
    }

    FlatFileItemReader<TransversalInput> transversalInputFlatFileItemReader() {
        final String[] fields = new String[]{"conceptType", "conceptUri", "skillType", "reuseLevel", "preferredLabel", "altLabels", "description", "broaderConceptUri", "broaderConceptPT"};

        return new FlatFileItemReaderBuilder<TransversalInput>()
                .name("ProcessTransversals Reader")
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(TransversalInput.class)
                .build();
    }

    @Bean
    public ItemWriter<TransversalInput> transversalInputItemWriter() {
        // TODO
        return items -> {
        };
    }

    @Data
    public static class TransversalInput {
        private String conceptType;
        private String conceptUri;
        private String skillType;
        private String reuseLevel;
        private String preferredLabel;
        private String altLabels;
        private String description;
        private String broaderConceptUri;
        private String broaderConceptPT;
    }
}
