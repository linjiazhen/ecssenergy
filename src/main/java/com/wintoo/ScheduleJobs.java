package com.wintoo;

import com.wintoo.monitor.RealCalEnergy;
import com.wintoo.monitor.TransmitMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@RestController
@Component
@EnableScheduling
public class ScheduleJobs {

    @Autowired
    @Qualifier("primaryJdbcTemplate")
    private JdbcOperations jdbcTemplateMysql;


    @Autowired
    @Qualifier("secondaryJdbcTemplate")
    private  JdbcOperations jdbcTemplateOracle;


    @Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private Map<String,ScheduledFuture<?>> futureMap=new HashMap<>();

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        return new ThreadPoolTaskScheduler();
    }



    @RequestMapping("/startTransmit")
    @Transactional(value = "primaryTransactionManager")
    public void startTransmit(){
        if(!futureMap.containsKey("Transmit"))
            futureMap.put("Transmit", threadPoolTaskScheduler.schedule(new TransmitMonitor(jdbcTemplateOracle,jdbcTemplateMysql), new CronTrigger("0/5 * * * * *")));
    }

    @RequestMapping("/stopTransmit")
    public void stopTransmit(){
        if (futureMap.containsKey("Transmit")) {
            futureMap.get("Transmit").cancel(true);
            futureMap.remove("Transmit");
        }
    }

    @RequestMapping("/startCalEnergy")
    @Transactional(value = "primaryTransactionManager")
    public void startCalEnergy(){
        if(!futureMap.containsKey("Transmit"))
            futureMap.put("CalEnergy", threadPoolTaskScheduler.schedule(new RealCalEnergy(jdbcTemplateMysql), new CronTrigger("0/5 * * * * *")));
    }

    @RequestMapping("/stopCalEnergy")
    public void stopCalEnergy(){
        if (futureMap.containsKey("CalEnergy")) {
            futureMap.get("CalEnergy").cancel(true);
            futureMap.remove("CalEnergy");
        }
    }
}
