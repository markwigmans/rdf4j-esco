package com.btb.rdf.esco.batch;

import com.btb.rdf.esco.config.ApplicationConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
@Primary
@RequiredArgsConstructor
public class BatchConfiguration extends DefaultBatchConfigurer {

    public static final String JOB_NAME = "multiThreadPagingBatch";

    private final ProcessSkills processSkills;
    private final ProcessSkillGroups processSkillGroups;
    private final ProcessOccupations processOccupations;
    private final ProcessISCOGroups processISCOGroups;
    private final ProcessBroaderRelations processBroaderRelations;
    private final ProcessSkillSkillRelation processSkillSkillRelation;
    private final ProcessOccupationalSkillRelation processOccupationalSkillRelation;
    private final ProcessTransversals processTransversals;

    private final ApplicationConfig config;
    private final JobLoggerListener jobLoggerListener;

    @Bean(JOB_NAME)
    public TaskExecutor taskExecutor() {
        int poolSize = config.getPoolSize();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setThreadNamePrefix("multi-thread-");
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get("ESCO job")
                .incrementer(new RunIdIncrementer())
                .start(processSkills.step())
//                .next(processSkillGroups.step())
//                .next(processTransversals.step())
//                .next(processSkillSkillRelation.step())
//                .next(processOccupations.step())
//                .next(processISCOGroups.step())
//                .next(processBroaderRelations.step())
//                .next(processOccupationalSkillRelation.step())
                .listener(jobLoggerListener)
                .build();
    }
}