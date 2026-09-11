package com.smarttmessenger.communicationwindow.controller;

import com.smarttmessenger.communicationwindow.entities.CommunicationWindowRequest;
import com.smarttmessenger.communicationwindow.entities.CommunicationWindowResponse;
import com.smarttmessenger.communicationwindow.entities.WindowMetadataResponse;
import com.smarttmessenger.communicationwindow.model.CommunicationWindow;
import com.smarttmessenger.communicationwindow.service.CommunicationWindowService;
import io.dropwizard.auth.Auth;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.util.List;
import java.util.Optional;
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

  /**
   * Called by the sender's app to show the window banner in a conversation.
   *
   * Takes any service identifier — a bare UUID for an ACI, or a {@code PNI:}-prefixed one — because
   * a sender does not always know the recipient's ACI. On a fresh install contacts are frequently
   * PNI-only until a profile fetch or the first exchanged message, and requiring an ACI here meant
   * the client could not ask at all and silently showed no banner. The service normalizes to the
   * recipient's ACI internally, exactly as the send path does, so the answer is identical either way.
   */
  @GET
  @Path("/metadata/{recipientServiceId}")
  public WindowMetadataResponse getMetadata(@Auth AuthenticatedDevice auth,
      @PathParam("recipientServiceId") String recipientServiceId) {
    final ServiceIdentifier identifier;
    try {
      identifier = ServiceIdentifier.valueOf(recipientServiceId);
    } catch (final IllegalArgumentException e) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    return service.getActiveWindowForSender(identifier)
        .map(info -> WindowMetadataResponse.active(
            info.startMinutes(), info.endMinutes(), info.name(), info.expectations()))
        .orElseGet(WindowMetadataResponse::inactive);
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
