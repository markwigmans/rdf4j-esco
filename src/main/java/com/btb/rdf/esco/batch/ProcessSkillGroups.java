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
public class ProcessSkillGroups {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;


    @Bean("ProcessSkillGroups.step")
    public Step step() {
        return this.stepBuilderFactory.get("Skill Groups")
                .transactionManager(tm)
                .<SkillGroup, SkillGroup>chunk(config.getChunkSize())
                .reader(skillGroupFlatFileItemReader())
                .writer(skillGroupItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<SkillGroup> skillGroupFlatFileItemReader() {
        final String[] fields = new String[]{"conceptType", "conceptUri", "preferredLabel", "altLabels", "hiddenLabels", "status", "modifiedDate", "scopeNote", "inScheme", "description", "code"};

        return new FlatFileItemReaderBuilder<SkillGroup>()
                .name("ProcessSkillGroups Reader")
                .resource(new ClassPathResource("skillGroups_nl.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(SkillGroup.class)
                .build();
    }

    @Bean
    public ItemWriter<SkillGroup> skillGroupItemWriter() {
        // TODO
        return items -> {
        };
    }

    @Data
    public static class SkillGroup {

        private String altLabels;
        private String code;
        private String conceptType;
        private String conceptUri;
        private String description;
        private String hiddenLabels;
        private String inScheme;
        private String modifiedDate;
        private String preferredLabel;
        private String scopeNote;
        private String status;
    }
}
