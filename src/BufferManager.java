import java.io.IOException;

public class BufferManager {

    private final DBConfig config;
    private final DiskManager diskManager;

    private final byte[][] bufferPool;
    private final FrameDescriptor[] frameDescriptors;
    private String currentPolicy;

    private static class FrameDescriptor {
        private PageId pageId;
        private int pin_count;
        private boolean dirty;

        FrameDescriptor() {
            this.pageId = null;
            this.pin_count = 0;
            this.dirty = false;
        }

        void reset() {
            this.pageId = null;
            this.pin_count = 0;
            this.dirty = false;
        }
    }

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.currentPolicy = config.getBmPolicy();

        int bufferCount = config.getBmBufferCount();
        this.bufferPool = new byte[bufferCount][config.getPagesize()];
        this.frameDescriptors = new FrameDescriptor[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            this.frameDescriptors[i] = new FrameDescriptor();
        }
    }

    
}
