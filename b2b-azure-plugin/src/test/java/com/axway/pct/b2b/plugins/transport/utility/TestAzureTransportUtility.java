package com.axway.pct.b2b.plugins.transport.utility;

import static org.junit.Assert.*;
import org.junit.*;

public class TestAzureTransportUtility {

    @Test
    public void testMetadataNameSanitization() {
        assertEquals("a", AzureTransportUtility.sanitizeMetadataName("a"));
        assertEquals("_1test", AzureTransportUtility.sanitizeMetadataName("1test"));
        assertEquals("te_st", AzureTransportUtility.sanitizeMetadataName("te.st"));
        assertEquals("_123te_st", AzureTransportUtility.sanitizeMetadataName("123te.st"));
        assertEquals("__000te__st_", AzureTransportUtility.sanitizeMetadataName(",000te..st,"));
    }
}
