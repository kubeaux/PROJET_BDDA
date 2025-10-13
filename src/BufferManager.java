import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {

    private final DBConfig config;
    private final DiskManager diskManager;
    private final int bufferCount;
    private ReplacementPolicy currentPolicy;
    private final BufferFrame[] frames;
    private final Map<PageId, BufferFrame> pageTable;
    private long usageCounter;

    private enum ReplacementPolicy {
        LRU;
        MRU;

        static ReplacementPolicy fromString(String policy) {
            if (policy == null) {
                return LRU;
            }
            switch (policy.trim().toUpperCase()) {
                case "LRU":
                    return LRU;
                case "MRU":
                    return MRU;
                default:
                    throw new IllegalArgumentException("Politique de remplacement inconnue : " + policy);
            }
        }
    }

    private static class BufferFrame {
        PageId pageId;
        final byte[] data;
        int pinCount;
        boolean dirty;
        long lastUsed;

        BufferFrame(int pageSize) {
            this.data = new byte[pageSize];
            this.pinCount = 0;
            this.dirty = false;
            this.lastUsed = 0;
        }

        void clear() {
            this.pageId = null;
            this.pinCount = 0;
            this.dirty = false;
            this.lastUsed = 0;
        }
    }

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.bufferCount = config.getBmBufferCount();
        this.currentPolicy = ReplacementPolicy.fromString(config.getBmPolicy());
        this.frames = new BufferFrame[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            frames[i] = new BufferFrame(config.getPagesize());
        }
        this.pageTable = new HashMap<>();
        this.usageCounter = 0;
    }

    public byte[] getPage(PageId pageId) throws IOException {
        BufferFrame frame = pageTable.get(pageId);
        usageCounter++;
        if (frame != null) {
            frame.pinCount++;
            frame.lastUsed = usageCounter;
            return frame.data;
        }

        BufferFrame target = findFrameForReplacement();
        if (target == null) {
            throw new IllegalStateException("Aucun buffer disponible pour charger la page" + pageId);
        }

        if (target.pageId != null) {
            if (target.dirty) {
                diskManager.WritePage(target.pageId, target.data);
            }
            pageTable.remove(target.pageId);
        }

        diskManager.ReadPage(pageId, target.data);
        target.pageId = pageId;
        target.pinCount = 1;
        target.dirty = false;
        target.lastUsed = usageCounter;
        pageTable.put(pageId, target);
        return target.data;
    }

    public void FreePage(PageId pageId, boolean valdirty) {
        BufferFrame frame = pageTable.get(pageId);
        if (frame == null) {
            throw new IllegalArgumentException("Page" + pageId + "non présente dans les buffers");
        }
        if (frame.pinCount <= 0) {
            throw new IllegalStateException("La page" + pageId + "est déjà libre");
        }

        frame.pinCount--;
        if (valdirty) {
            frame.dirty = true;
        }
    }

    public void SetCurrentReplacementPolicy(String policy) {
        this.currentPolicy = ReplacementPolicy.fromString(policy);
    }

    public void FlushBuffers() throws IOException {
        for (BufferFrame frame : frames) {
            if (frame.pageId != null) {
                if (frame.dirty) {
                    diskManager.WritePage(frame.pageId, frame.data);
                }
                pageTable.remove(frame.pageId);
                frame.clear();
            }
        }
    }

    private BufferFrame findFrameForReplacement() {
        BufferFrame unused = null;
        for (BufferFrame frame : frames) {
            if (frame.pageId == null) {
                unused = frame;
                break;
            }
        }
        if (unused != null) {
            return unused;
        }

        BufferFrame candidate = null;
        for (BufferFrame frame : frames) {
            if (frame.pinCount == 0) {
                if (candidate == null) {
                    candidate = frame;
                } else {
                    if (currentPolicy == ReplacementPolicy.LRU) {
                        if (frame.lastUsed < candidate.lastUsed) {
                            candidate = frame;
                        } else {
                            if (frame.lastUsed > candidate.lastUsed) {
                                candidate = frame;
                            }
                        }
                    }
                }
            }
        }
        return candidate;
    }
}
