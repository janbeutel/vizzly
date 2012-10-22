/*
 * Copyright 2012 ETH Zurich, Computer Engineering and Networks Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.vizzly.performance;

/**
 * This class defines a dummy performance tracker that can be used instead of the DbPerformanceTracker
 * when no MySQL database is involved.
 * @author Matthias Keller
 *
 */
public class DummyPerformanceTracker extends AbstractPerformanceTracker {

    public void addDataFetchMeasurement(DataFetchPerformanceMeasurement p) {
        return;
    }
    
    public void addUserRequestMeasurement(UserRequestPerformanceMeasurement p) {
        return;
    }

    public void stopApplication() {
        return;
    }
    
}
