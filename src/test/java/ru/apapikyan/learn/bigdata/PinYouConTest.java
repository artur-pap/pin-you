package ru.apapikyan.learn.bigdata;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple PinYouCon.
 */
public class PinYouConTest extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public PinYouConTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(PinYouConTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() {
		assertTrue(true);
		// InternalConfig.training2FileNames
		// assertEquals("bid.20130606.txt.bz2", "");
		// assertEquals("bid.20130607.txt.bz2", "");
		// assertEquals("bid.20130608.txt.bz2", "");
		// assertEquals("bid.20130609.txt.bz2", "");
		// assertEquals("bid.20130610.txt.bz2", "");
		// assertEquals("bid.20130611.txt.bz2", "");
		// assertEquals("bid.20130612.txt.bz2", "");
	}
}
