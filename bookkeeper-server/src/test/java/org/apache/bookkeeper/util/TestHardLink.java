package org.apache.bookkeeper.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestHardLink {

    private File tempDir;

    @Before
    public void setup() throws IOException {
        // Create at least one file so that target disk will never be empty
        tempDir = IOUtils.createTempDir("TestHardLink", "test-hardlink");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(tempDir);
    }

    private void verifyHardLink(File origin, File linkedOrigin) throws IOException {
        Assert.assertTrue(origin.exists());
        Assert.assertFalse(linkedOrigin.exists());

        HardLink.createHardLink(origin, linkedOrigin);

        Assert.assertTrue(origin.exists());
        Assert.assertTrue(linkedOrigin.exists());

        // when delete origin file it should be success and not exist.
        origin.delete();
        Assert.assertFalse(origin.exists());
        Assert.assertTrue(linkedOrigin.exists());
    }

    @Test
    public void testHardLink() throws IOException {
        String uuidSuffix = UUID.randomUUID().toString();

        // prepare file
        File origin = new File(tempDir, "originFile." + uuidSuffix);
        File linkedOrigin = new File(tempDir, "linkedOrigin." + uuidSuffix);
        origin.createNewFile();

        // disable jdk api link first
        HardLink.enableJdkLinkApi(false);
        verifyHardLink(origin, linkedOrigin);

        // prepare file
        File jdkorigin = new File(tempDir, "jdkoriginFile." + uuidSuffix);
        File jdklinkedOrigin = new File(tempDir, "jdklinkedOrigin." + uuidSuffix);
        jdkorigin.createNewFile();

        // enable jdk api link
        HardLink.enableJdkLinkApi(true);
        verifyHardLink(jdkorigin, jdklinkedOrigin);
    }
}
