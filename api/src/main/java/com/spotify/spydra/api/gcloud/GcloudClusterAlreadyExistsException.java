package com.spotify.spydra.api.gcloud;

public class GcloudClusterAlreadyExistsException extends RuntimeException {

  public GcloudClusterAlreadyExistsException(String msg) {
    super(msg);
  }
}
