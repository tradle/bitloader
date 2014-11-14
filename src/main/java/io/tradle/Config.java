package io.tradle;

public class Config {

	private String serverAlias;
  private String serverName;
  private String indexServerName;
	private AddressConfig loaderAddress;
	
	public String serverAlias() {
		return serverAlias;
	}
  public String serverName() {
    return serverName;
  }
  public String indexServerName() {
    return indexServerName;
  }
	
	public AddressConfig address() {
		return loaderAddress;
	}
	
	public static class AddressConfig {
		private String host;
		private String path = "";
		private int port;

		public String address() {
			return host;
		}
		
		public String path() {
			return path;
		}
		
		public int port() {
			return port;
		}
		
		@Override
		public String toString() {
			return host + ":" + port + "/" + path;
		}
	}
	
}
