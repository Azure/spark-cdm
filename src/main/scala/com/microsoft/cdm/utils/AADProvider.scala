package com.microsoft.cdm.utils

import com.microsoft.aad.adal4j.AuthenticationContext
import java.util.concurrent.Executors
import com.microsoft.aad.adal4j.ClientCredential

class AADProvider(appId: String, appKey: String, tenantId: String) extends Serializable  {

  // TODO: cache token

  val storageResource: String = "https://storage.azure.com/"
  val authorityFormat = "https://login.microsoftonline.com/%s/oauth2/token"

  var getToken: String = {
    val authority = authorityFormat.format(tenantId)
    val service = Executors.newFixedThreadPool(1)
    val context = new AuthenticationContext(authority, true, service)
    val future = context.acquireToken(storageResource, new ClientCredential(appId, appKey), null)
    "Bearer " + future.get().getAccessToken
  }

}
