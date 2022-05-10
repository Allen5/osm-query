package org.yunqiacademy.centralsystem.plugins.lifecycle;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.yunqiacademy.centralsystem.kernel.proto.pluginmaster.PluginMasterService;
import org.yunqiacademy.centralsystem.kernel.proto.pluginmaster.PluginRegisterRequest;
import org.yunqiacademy.centralsystem.kernel.proto.pluginmaster.PluginUnRegisterRequest;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
@Slf4j
public class LifeCycle {

    @GrpcClient("plugin-master-service")
    PluginMasterService pluginMasterService;

    @ConfigProperty(name = "plugin.service.host")
    String grpcHost;

    @ConfigProperty(name = "quarkus.grpc.server.port")
    String grpcPort;

    @ConfigProperty(name = "plugin.protocol")
    String protocol;

    /**
     * plugin启动时向master service注册
     * @param ev
     */
    void onStart(@Observes StartupEvent ev) {
        log.info("odps plugin is starting");

        PluginRegisterRequest request = PluginRegisterRequest.newBuilder()
            .setEndpoint(grpcHost + ":" + grpcPort)
            .setProtocol(protocol)
            .build();
        pluginMasterService.register(request)
            .subscribe()
            .with(
                pluginUnRegisterResponse -> {
                    log.info("register result: {}", pluginUnRegisterResponse.getResult());
                },
                throwable -> {
                    log.error("register request failed: {}", throwable.getMessage());
                }
            );
    }

    /**
     * plugin停止时向master service取消注册
     * @param ev
     */
    void onStop(@Observes ShutdownEvent ev) {
        log.info("odps plugin is stopping");

        PluginUnRegisterRequest request = PluginUnRegisterRequest.newBuilder()
            .setEndpoint(grpcHost + ":" + grpcPort)
            .setProtocol(protocol)
            .build();
        pluginMasterService.unregister(request)
            .subscribe()
            .with(
                pluginUnRegisterResponse -> {
                    log.info("unregister result: {}", pluginUnRegisterResponse.getResult());
                },
                throwable -> {
                    log.error("unregister request failed: {}", throwable.getMessage());
                }
            );
    }
}
