/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.concurrent._

private[spark] class SparkListenerClusterPressure extends SparkListenerEvent {

}

private[spark] class ClusterPressureMonitor(
  scheduler: SchedulerBackend,
  listenerBus: LiveListenerBus
) extends SparkListenerBus {
  private var thread: Thread = _
  
  def start(): Unit = {
    thread = new Thread("cluster-pressure-thread") {
      override def run(): Unit = {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(10 * 1000);
          } catch {
            case e: InterruptedException => null
          }
          println("CHECK")
          if (scheduler.isClusterPressure()) {
            println("PRESSURE")
            listenerBus.post(new SparkListenerClusterPressure)
          }
        }
        println("STOPPED")
      }
    }
    thread.setDaemon(true);
    thread.start
  }
  
  def stop(wait: Boolean = false): Unit = {
    thread.interrupt
    if (wait) thread.join
    thread = null
  }
}

