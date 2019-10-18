package open.quartz.store.couchbase;

import static org.junit.Assert.assertEquals;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.triggers.SimpleTriggerImpl;

public class CouchbaseStoreTest {
	private CouchbaseStore store;
	
	@Before
	public void setup() throws IOException, JobPersistenceException {
		store = new CouchbaseStore();
		store.setProperties("/couchbase-quartz.properties");
		store.clearAllSchedulingData();
	}
	
	@Test
    public void shouldDeleteAllJobsTriggersAndCalendars() throws JobPersistenceException {
        String jobId = id("fooid");
        String group = id("bargroup");
		JobDetail job = newJob(DummyJob.class)
                .withIdentity(jobId, group)
                .usingJobData("foo", "bar")
                .build();
        store.storeJob(job, false);
        assertEquals("bar", store.retrieveJob(JobKey.jobKey(jobId, group)).getJobDataMap().get("foo"));
        
        Calendar testCalendar = new WeeklyCalendar();
        String calendarName = id("weeklyCalendar");
        store.storeCalendar(calendarName, testCalendar, false, false);
        Calendar dbCalendar = store.retrieveCalendar(calendarName);
        assertEquals(testCalendar.getClass(), dbCalendar.getClass());
        
        String triggerId = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerId, group)
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .modifiedByCalendar(calendarName)
                .build();
        store.storeTrigger(trigger, false);
        assertEquals(new Date(2010, 10, 20), store.retrieveTrigger(TriggerKey.triggerKey(triggerId, group)).getStartTime());

        store.clearAllSchedulingData();

        assertEquals(0, store.getNumberOfJobs());
        assertEquals(0, store.getNumberOfCalendars());
        assertEquals(0, store.getNumberOfTriggers());
        assertEquals(0, store.getJobGroupNames().size());
        assertEquals(0, store.getCalendarNames().size());
    }
	
	public static String id(String name) {
        return name + "-" + UUID.randomUUID();
    }
}
