package com.microsoft.cdm.utils

import com.microsoft.aad.adal4j.AuthenticationContext
import java.util.concurrent.Executors
import com.microsoft.aad.adal4j.ClientCredential

/**
  * Abstraction for getting tokens from AAD.
  * @param appId AAD application id.
  * @param appKey AAD application key.
  * @param tenantId AAD tenant to authenticate under.
  */
class AADProvider(appId: String, appKey: String, tenantId: String) extends Serializable  {

  // TODO: cache token

  val storageResource: String = "https://storage.azure.com/"
  val authorityFormat = "https://login.microsoftonline.com/%s/oauth2/token"

  /**
    * Called to get an AAD token for authorization.
    * @return An AAD Bearer token for the specified service principal.
    */
  def getToken: String = {
    val authority = authorityFormat.format(tenantId)
    val service = Executors.newFixedThreadPool(1)
    val context = new AuthenticationContext(authority, true, service)
    val future = context.acquireToken(storageResource, new ClientCredential(appId, appKey), null)
    "Bearer " + future.get().getAccessToken
  }

}
