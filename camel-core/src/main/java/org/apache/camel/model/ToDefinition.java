/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.processor.SendAsyncProcessor;
import org.apache.camel.processor.UnitOfWorkProcessor;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.util.concurrent.ExecutorServiceHelper;

/**
 * Represents an XML &lt;to/&gt; element
 *
 * @version $Revision$
 */
@XmlRootElement(name = "to")
@XmlAccessorType(XmlAccessType.FIELD)
public class ToDefinition extends SendDefinition<ToDefinition> implements ExecutorServiceAwareDefinition<ToDefinition> {
    @XmlTransient
    private final List<ProcessorDefinition> outputs = new ArrayList<ProcessorDefinition>();
    @XmlAttribute(required = false)
    private ExchangePattern pattern;
    @XmlAttribute(required = false)
    @Deprecated
    private Boolean async = Boolean.FALSE;
    @XmlTransient
    @Deprecated
    private ExecutorService executorService;
    @XmlAttribute(required = false)
    @Deprecated
    private String executorServiceRef;
    @XmlAttribute(required = false)
    @Deprecated
    private Integer poolSize;

    public ToDefinition() {
    }

    public ToDefinition(String uri) {
        setUri(uri);
    }

    public ToDefinition(Endpoint endpoint) {
        setEndpoint(endpoint);
    }

    public ToDefinition(String uri, ExchangePattern pattern) {
        this(uri);
        this.pattern = pattern;
    }

    public ToDefinition(Endpoint endpoint, ExchangePattern pattern) {
        this(endpoint);
        this.pattern = pattern;
    }

    @Override
    public List<ProcessorDefinition> getOutputs() {
        return outputs;
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        if (async == null || !async) {
            // when sync then let super create the processor
            return super.createProcessor(routeContext);
        }

        // this code below is only for creating when async is enabled
        // ----------------------------------------------------------

        // create the child processor which is the async route
        Processor childProcessor = this.createChildProcessor(routeContext, false);

        // wrap it in a unit of work so the route that comes next is also done in a unit of work
        UnitOfWorkProcessor uow = new UnitOfWorkProcessor(routeContext, childProcessor);

        // create async processor
        Endpoint endpoint = resolveEndpoint(routeContext);

        // TODO: rework to have configured executor service in SendAsyncProcessor being handled in stop/start scenario

        SendAsyncProcessor async = new SendAsyncProcessor(endpoint, getPattern(), uow);

        executorService = ExecutorServiceHelper.getConfiguredExecutorService(routeContext, "ToAsync", this);
        if (executorService != null) {
            async.setExecutorService(executorService);
        }
        if (poolSize != null) {
            async.setPoolSize(poolSize);
        }

        return async;
    }

    @Override
    public String toString() {
        if (async != null && async) {
            return "ToAsync[" + getLabel() + "] -> " + getOutputs();
        } else {
            return "To[" + getLabel() + "]";
        }
    }

    @Override
    public String getShortName() {
        return "to";
    }

    @Override
    public ExchangePattern getPattern() {
        return pattern;
    }

    @Deprecated
    public Boolean isAsync() {
        return async;
    }

    @Deprecated
    public void setAsync(Boolean async) {
        this.async = async;
    }

    @Deprecated
    public Integer getPoolSize() {
        return poolSize;
    }

    @Deprecated
    public void setPoolSize(Integer poolSize) {
        this.poolSize = poolSize;
    }

    @Deprecated
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Deprecated
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Deprecated
    public String getExecutorServiceRef() {
        return executorServiceRef;
    }

    @Deprecated
    public void setExecutorServiceRef(String executorServiceRef) {
        this.executorServiceRef = executorServiceRef;
    }

    /**
     * Sets the optional {@link ExchangePattern} used to invoke this endpoint
     */
    public void setPattern(ExchangePattern pattern) {
        this.pattern = pattern;
    }

    /**
     * Sets the optional {@link ExchangePattern} used to invoke this endpoint
     */
    public ToDefinition pattern(ExchangePattern pattern) {
        setPattern(pattern);
        return this;
    }

    @Deprecated
    public ToDefinition executorService(ExecutorService executorService) {
        setExecutorService(executorService);
        return this;
    }

    @Deprecated
    public ToDefinition executorServiceRef(String executorServiceRef) {
        setExecutorServiceRef(executorServiceRef);
        return this;
    }

    /**
     * Setting the core pool size for the underlying {@link java.util.concurrent.ExecutorService}.
     *
     * @return the builder
     */
    @Deprecated
    public ToDefinition poolSize(int poolSize) {
        setPoolSize(poolSize);
        return this;
    }
}
