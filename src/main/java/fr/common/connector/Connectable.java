package fr.common.connector;

/**
 * Abstraction Interface for data connectors
 * 
 * @author ahmed-externe.dridi@edf.fr
 */

public interface Connectable {
	// --------------------------------------------------
	// ABSTRACTION
	// --------------------------------------------------

	/**
	 * Data connector connection operation
	 */
	public void connect();

	/**
	 * Data connector disconnection operation
	 */
	public void disconnect();

}
