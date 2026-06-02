package com.smarttmessenger.communicationwindow.controller;

import com.smarttmessenger.communicationwindow.entities.CommunicationWindowRequest;
import com.smarttmessenger.communicationwindow.entities.CommunicationWindowResponse;
import com.smarttmessenger.communicationwindow.entities.WindowMetadataResponse;
import com.smarttmessenger.communicationwindow.model.CommunicationWindow;
import com.smarttmessenger.communicationwindow.model.CommunicationWindowSchedule;
import com.smarttmessenger.communicationwindow.service.CommunicationWindowService;
import io.dropwizard.auth.Auth;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Path("/v1/smartt/communication-windows")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CommunicationWindowController {

  private final CommunicationWindowService service;
  private final AccountsManager accountsManager;

  public CommunicationWindowController(CommunicationWindowService service,
      AccountsManager accountsManager) {
    this.service = service;
    this.accountsManager = accountsManager;
  }

  @GET
  public List<CommunicationWindowResponse> listWindows(@Auth AuthenticatedDevice auth) {
    String accountUuid = auth.accountIdentifier().toString();
    return service.getWindows(accountUuid).stream()
        .map(CommunicationWindowResponse::from)
        .collect(Collectors.toList());
  }

  @POST
  public Response createWindow(@Auth AuthenticatedDevice auth,
      @NotNull @Valid CommunicationWindowRequest request) {
    String accountUuid = auth.accountIdentifier().toString();
    CommunicationWindow window = fromRequest(request);
    CommunicationWindow created = service.createWindow(accountUuid, window);
    return Response.ok(CommunicationWindowResponse.from(created)).build();
  }

  @PUT
  @Path("/{windowId}")
  public Response updateWindow(@Auth AuthenticatedDevice auth,
      @PathParam("windowId") String windowId,
      @NotNull @Valid CommunicationWindowRequest request) {
    String accountUuid = auth.accountIdentifier().toString();
    Optional<CommunicationWindow> updated = service.updateWindow(accountUuid, windowId, fromRequest(request));
    return updated
        .map(w -> Response.ok(CommunicationWindowResponse.from(w)).build())
        .orElse(Response.status(Response.Status.NOT_FOUND).build());
  }

  @DELETE
  @Path("/{windowId}")
  public Response deleteWindow(@Auth AuthenticatedDevice auth,
      @PathParam("windowId") String windowId) {
    String accountUuid = auth.accountIdentifier().toString();
    return service.deleteWindow(accountUuid, windowId)
        ? Response.noContent().build()
        : Response.status(Response.Status.NOT_FOUND).build();
  }

  /** Called by the sender's app to show the window banner in a conversation. */
  @GET
  @Path("/metadata/{recipientAci}")
  public WindowMetadataResponse getMetadata(@Auth AuthenticatedDevice auth,
      @PathParam("recipientAci") UUID recipientAci) {
    AciServiceIdentifier identifier = new AciServiceIdentifier(recipientAci);
    Optional<CommunicationWindow> active = service.getActiveWindowForSender(identifier);

    return active.map(window -> {
      CommunicationWindowSchedule schedule = window.getSchedules().stream()
          .filter(s -> {
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            return s.isCurrentlyActive(now);
          })
          .findFirst()
          .orElse(null);

      int start = schedule != null ? schedule.getStart() : 0;
      int end = schedule != null ? schedule.getEnd() : 0;

      return WindowMetadataResponse.active(start, end, window.getName(), window.getExpectations());
    }).orElseGet(WindowMetadataResponse::inactive);
  }

  private CommunicationWindow fromRequest(CommunicationWindowRequest r) {
    return new CommunicationWindow(
        null,
        r.getName(),
        r.getEmoji(),
        r.isEnabled(),
        r.getSchedules(),
        r.getExceptionContacts(),
        r.isAllowCallsFromExceptions(),
        r.isAllowCallsFromAll(),
        r.getExpectations()
    );
  }
}
