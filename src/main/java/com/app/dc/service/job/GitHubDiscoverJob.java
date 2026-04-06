package com.app.dc.service.job;

import com.app.dc.service.BatchStart;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@Slf4j
public class GitHubDiscoverJob implements Job {

    private static boolean runFlag = false;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        Object o = jobDataMap.get("Object");
        if (o instanceof Object[]) {
            Object[] objs = (Object[]) o;
            BatchStart batchStart = (BatchStart) objs[0];
            runJob(batchStart);
        }
    }

    public synchronized void runJob(BatchStart batchStart) {
        if (runFlag) {
            log.warn("GitHubDiscoverJob is runing.");
            return;
        }
        runFlag = true;
        try {
            batchStart.runGitHubDiscoverJob();
        } finally {
            runFlag = false;
        }
    }
}
