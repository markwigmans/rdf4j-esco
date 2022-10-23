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
public class ProcessISCOGroups {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;

    @Bean("ProcessISCOGroups.step")
    public Step step() {
        return this.stepBuilderFactory.get("ISCO Groups")
                .transactionManager(tm)
                .<ISCOGroup, ISCOGroup>chunk(config.getChunkSize())
                .reader(iscoGroupFlatFileItemReader())
                .writer(iscoGroupItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<ISCOGroup> iscoGroupFlatFileItemReader() {
        final String[] fields = new String[]{"conceptType", "conceptUri", "code", "preferredLabel", "altLabels", "inScheme", "description"};

        return new FlatFileItemReaderBuilder<ISCOGroup>()
                .name("ProcessISCOGroups Reader")
                .resource(new ClassPathResource("ISCOGroups_nl.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(ISCOGroup.class)
                .build();
    }

    @Bean
    public ItemWriter<ISCOGroup> iscoGroupItemWriter() {
        // TODO
        return items -> {
        };
    }

    @Data
    public static class ISCOGroup {

        private String altLabels;
        private String code;
        private String conceptType;
        private String conceptUri;
        private String description;
        private String inScheme;
        private String preferredLabel;
    }
}
