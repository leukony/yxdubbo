/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;
import com.alibaba.dubbo.rpc.support.RpcUtils;

/**
 * 失败转移，当出现失败，重试其它服务器，通常用于读操作，但重试会带来更长延迟。
 * 
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 * 
 * @author william.liangf
 * @author chao.liuc
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        com.yunxi.common.tracer.tracer.RpcTracer rpcTracer = com.yunxi.common.tracer.TracerFactory.getRpcClientTracer();
        com.yunxi.common.tracer.context.RpcContext rpcContext = startInvokeWithTracer(invocation, rpcTracer);
        try {
            List<Invoker<T>> copyinvokers = invokers;
        	checkInvokers(copyinvokers, invocation);
            int len = getUrl().getMethodParameter(invocation.getMethodName(), Constants.RETRIES_KEY, Constants.DEFAULT_RETRIES) + 1;
            if (len <= 0) {
                len = 1;
            }
            // retry loop.
            RpcException le = null; // last exception.
            List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyinvokers.size()); // invoked invokers.
            Set<String> providers = new HashSet<String>(len);
            for (int i = 0; i < len; i++) {
            	//重试时，进行重新选择，避免重试时invoker列表已发生变化.
            	//注意：如果列表发生了变化，那么invoked判断会失效，因为invoker示例已经改变
            	if (i > 0) {
            	    rpcContext = startInvokeWithTracer(invocation, rpcTracer);
            		checkWheatherDestoried();
            		copyinvokers = list(invocation);
            		//重新检查一下
            		checkInvokers(copyinvokers, invocation);
            	}
                Invoker<T> invoker = select(loadbalance, invocation, copyinvokers, invoked);
                invoked.add(invoker);
                RpcContext.getContext().setInvokers((List)invoked);
                rpcContext.setTargetIP(invoker.getUrl().getHost());
                try {
                    Result result = invoker.invoke(invocation);
                    if (le != null && logger.isWarnEnabled()) {
                        logger.warn("Although retry the method " + invocation.getMethodName()
                                + " in the service " + getInterface().getName()
                                + " was successful by the provider " + invoker.getUrl().getAddress()
                                + ", but there have been failed providers " + providers 
                                + " (" + providers.size() + "/" + copyinvokers.size()
                                + ") from the registry " + directory.getUrl().getAddress()
                                + " on the consumer " + NetUtils.getLocalHost()
                                + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                                + le.getMessage(), le);
                    }
                    finishInvokeWithTracerOnSuccess(rpcTracer);
                    return result;
                } catch (RpcException e) {
                    if (e.isBiz()) { // biz exception.
                        throw e;
                    }
                    le = e;
                    if (i < len - 1) {
                        finishInvokeWithTracerOnException(le, rpcTracer);
                    }
                } catch (Throwable e) {
                    le = new RpcException(e.getMessage(), e);
                    if (i < len - 1) {
                        finishInvokeWithTracerOnException(le, rpcTracer);
                    }
                } finally {
                    providers.add(invoker.getUrl().getAddress());
                }
            }
            throw new RpcException(le != null ? le.getCode() : 0, "Failed to invoke the method "
                    + invocation.getMethodName() + " in the service " + getInterface().getName() 
                    + ". Tried " + len + " times of the providers " + providers 
                    + " (" + providers.size() + "/" + copyinvokers.size() 
                    + ") from the registry " + directory.getUrl().getAddress()
                    + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                    + Version.getVersion() + ". Last error is: "
                    + (le != null ? le.getMessage() : ""), le != null && le.getCause() != null ? le.getCause() : le);
        } catch (RpcException e) {
            finishInvokeWithTracerOnException(e, rpcTracer);
            throw e;
        }
    }
    
    /**
     * 
     * 
     * @param invocation
     * @param rpcTracer
     * @return
     */
    private com.yunxi.common.tracer.context.RpcContext startInvokeWithTracer(Invocation invocation, com.yunxi.common.tracer.tracer.RpcTracer rpcTracer) {
        com.yunxi.common.tracer.context.RpcContext rpcContext = rpcTracer.startInvoke();
        if (rpcContext != null) {
            RpcContext.getContext().setAttachment(com.yunxi.common.tracer.constants.TracerConstants.TRACE_ID, rpcContext.getTraceId());
            RpcContext.getContext().setAttachment(com.yunxi.common.tracer.constants.TracerConstants.RPC_ID, rpcContext.getRpcId());
            
            rpcContext.setServiceName(getUrl().getServiceKey());
            rpcContext.setMethodName(RpcUtils.getMethodName(invocation));
            rpcContext.setCallIP(NetUtils.getLocalHost());
            rpcContext.setCallApp(getUrl().getParameter(Constants.APPLICATION_KEY));
            rpcContext.setCurrentApp(getUrl().getParameter(Constants.APPLICATION_KEY));
            rpcContext.setProtocol(getUrl().getProtocol());
                
            if (RpcUtils.isOneway(getUrl(), invocation)) {
                rpcContext.setRpcType(com.yunxi.common.tracer.constants.RpcType.ONEWAY.getType());
            } else if (RpcUtils.isAsync(getUrl(), invocation)) {
                rpcContext.setRpcType(com.yunxi.common.tracer.constants.RpcType.ASYNC.getType());
            } else {
                rpcContext.setRpcType(com.yunxi.common.tracer.constants.RpcType.SYNC.getType());
            }
        }
        return rpcContext;
    }
    
    /**
     * 
     * 
     * @param rpcTracer
     */
    private void finishInvokeWithTracerOnSuccess(com.yunxi.common.tracer.tracer.RpcTracer rpcTracer) {
        rpcTracer.finishInvoke(com.yunxi.common.tracer.constants.RpcCode.RPC_SUCCESS.getCode(), 
            com.yunxi.common.tracer.context.RpcContext.class);
    }
    
    /**
     * 
     * 
     * @param e
     * @param rpcTracer
     */
    private void finishInvokeWithTracerOnException(RpcException e, com.yunxi.common.tracer.tracer.RpcTracer rpcTracer) {
        if (e.isNetwork()) {
            rpcTracer.finishInvoke(com.yunxi.common.tracer.constants.RpcCode.RPC_NETWORK_FAILED.getCode(), 
                com.yunxi.common.tracer.context.RpcContext.class);
        } else if (e.isTimeout()) {
            rpcTracer.finishInvoke(com.yunxi.common.tracer.constants.RpcCode.RPC_TIMEOUT_FAILED.getCode(), 
                com.yunxi.common.tracer.context.RpcContext.class);
        } else {
            rpcTracer.finishInvoke(com.yunxi.common.tracer.constants.RpcCode.RPC_BIZ_FAILED.getCode(), 
                com.yunxi.common.tracer.context.RpcContext.class);
        }
    }
}