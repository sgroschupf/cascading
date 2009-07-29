/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.stats;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.mapred.Counters;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStep.FlowStepJob;


/** Class FlowStats collects {@link Flow} specific statistics. */
public class FlowStats extends CascadingStats
  {
  /** Field stepsCount */
  int stepsCount;
  private Map<String, Callable<Throwable>> jobsMap;
  private CountDownLatch latch = new CountDownLatch(1);

  /**
   * Method getStepsCount returns the number of steps this Flow executed.
   *
   * @return the stepsCount (type int) of this FlowStats object.
   */
  public int getStepsCount()
    {
    return stepsCount;
    }

  /**
   * Method setStepsCount sets the steps value.
   *
   * @param stepsCount the stepsCount of this FlowStats object.
   * 
   * @deprecated SG: since we pass in the complete jobsMap now we might not need to set this separately.
   */
  public void setStepsCount( int stepsCount )
    {
    this.stepsCount = stepsCount;
    }

  @Override
  protected String getStatsString()
    {
    return super.getStatsString() + ", stepsCount=" + stepsCount;
    }

  @Override
  public String toString()
    {
    return "Flow{" + getStatsString() + '}';
    }

  public void setJobsMap(Map<String, Callable<Throwable>> jobsMap) 
    {
	this.jobsMap = jobsMap;
	latch.countDown();
    }
  
	/**
	 * @return names of flow jobs or null if not yet known and we don't wait for it.
	 */
  public String[] getJobNames(boolean  wait) 
    {
	  if(wait)
	  {
		  try {
			latch.await();
		} catch (InterruptedException e) {
			return null;
		}
	  }
	  if(jobsMap != null)
	  {
		  return jobsMap.keySet().toArray(new String[jobsMap.size()]);
	  } else {
		  return null;
	  }
	  
    }
  
  public Counters getCounter(String jobName, boolean wait) throws IOException, InterruptedException 
    {
	  if(wait)
	  {
		try {
			latch.await();
		} catch (InterruptedException e) {
			return null;
		}
	  }
	  if(jobsMap!=null)
	  {
		 FlowStep.FlowStepJob job = (FlowStepJob) jobsMap.get(jobName);
	  if(job != null)
	  	{
		  return job.getCounters();
		}  
	  }
	  return null;
    }
  
  }
