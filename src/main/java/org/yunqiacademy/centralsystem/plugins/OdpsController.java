package org.yunqiacademy.centralsystem.plugins;

import lombok.extern.slf4j.Slf4j;
import org.yunqiacademy.centralsystem.plugins.service.OdpsService;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/api")
@Slf4j
public class OdpsController {

    @Inject
    private OdpsService odpsService;

    @Path("/getData")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public String getOdpsData() {
        return odpsService.getOdpsData("shanghai", 125.1851, 125.4295, 43.7725, 43.9277).toString();
    }
}
