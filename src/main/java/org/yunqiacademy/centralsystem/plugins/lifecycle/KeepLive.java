package org.yunqiacademy.centralsystem.plugins.lifecycle;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.scheduler.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.yunqiacademy.centralsystem.kernel.proto.pluginmaster.PluginKeepAliveRequest;
import org.yunqiacademy.centralsystem.kernel.proto.pluginmaster.PluginMasterService;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Slf4j
public class KeepLive {
    
    @GrpcClient("plugin-master-service")
    PluginMasterService pluginMasterService;

    @ConfigProperty(name = "plugin.service.host")
    String grpcHost;

    @ConfigProperty(name = "quarkus.grpc.server.port")
    String grpcPort;

    @ConfigProperty(name = "plugin.protocol")
    String protocol;

    /**
     * 向plugin master发送心跳
     */
    @Scheduled(every = "10s")
    void keepLive() {
        log.info("keeplive");

        //调用指定endpoint的插件暴露的方法，通过插件获取目标数据
        PluginKeepAliveRequest req = PluginKeepAliveRequest.newBuilder()
            .setEndpoint(grpcHost + ":" + grpcPort)
            .setProtocol(protocol)
            .build();
        pluginMasterService.keepAlive(req)
            .subscribe()
            .with(
                pluginKeepAliveResponse -> {
                    log.info("keeplive result: {}", pluginKeepAliveResponse.getResult());
                }, throwable -> {
                    log.error("keeplive request error: {}", throwable.getMessage());
                }
            );
    }
}
