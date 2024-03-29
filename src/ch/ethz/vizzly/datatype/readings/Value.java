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

package ch.ethz.vizzly.datatype.readings;

import java.io.Serializable;

/**
 * This class implements a data type that represents a single measurement (value).
 * @author Matthias Keller
 *
 */
public class Value {
    
    public double value;
    
    protected static final double NULL_VALUE = -9999;
    
    public Value() {
        value = NULL_VALUE;
    }
    
    public Value(double value) {
        this.value = value;
    }
    
    public Boolean isNull() {
        return value == NULL_VALUE;
    }
    
}
