package com.example.batchprocessing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {
    private final JobLauncher jobLauncher;
    private final Job myJob;

    public BatchController(JobLauncher jobLauncher, Job myJob) {
        this.jobLauncher = jobLauncher;
        this.myJob = myJob;
    }

    @PostMapping("/start_processing")
    public ResponseEntity<String> start() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .toJobParameters();
        jobLauncher.run(myJob, params);
        return ResponseEntity.ok("Success");
    }
}