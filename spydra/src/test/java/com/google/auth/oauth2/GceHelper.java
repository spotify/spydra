package com.google.auth.oauth2;

public class GceHelper {

  public static boolean runningOnComputeEngine() {

    return ComputeEngineCredentials.runningOnComputeEngine(OAuth2Utils.HTTP_TRANSPORT_FACTORY,
        DefaultCredentialsProvider.DEFAULT);
  }
}
