package com.btb.rdf.esco.batch;

import com.btb.rdf.esco.config.ApplicationConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ProcessOccupations {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;

    @Bean("ProcessOccupations.step")
    public Step step() {
        return this.stepBuilderFactory.get("Occupations")
                .transactionManager(tm)
                .<Occupation, Occupation>chunk(config.getChunkSize())
                .reader(occupationFlatFileItemReader())
                .writer(occupationItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<Occupation> occupationFlatFileItemReader() {
        final String[] fields = new String[]{"conceptType", "conceptUri", "iscoGroup", "preferredLabel", "altLabels", "hiddenLabels", "status", "modifiedDate", "regulatedProfessionNote", "scopeNote", "definition", "inScheme", "description", "code"};

        return new FlatFileItemReaderBuilder<Occupation>()
                .name("ProcessOccupations Reader")
                .resource(new ClassPathResource("occupations_nl.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(Occupation.class)
                .build();
    }

    @Bean
    public ItemWriter<Occupation> occupationItemWriter() {
        // TODO
        return items -> {
        };
    }

    @Data
    public static class Occupation {

        private String altLabels;
        private String code;
        private String conceptType;
        private String conceptUri;
        private String definition;
        private String description;
        private String hiddenLabels;
        private String inScheme;
        private String iscoGroup;
        private String modifiedDate;
        private String preferredLabel;
        private String regulatedProfessionNote;
        private String scopeNote;
        private String status;
    }
}
