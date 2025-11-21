package minisgbd;

import java.util.Objects;

/**
 * Identifiant d'un record : (pageId, slotIdx)
 */
public class RecordId {
    private PageId pageId;
    private int slotIdx;

    public RecordId(PageId pageId, int slotIdx) {
        if (pageId == null) throw new IllegalArgumentException("pageId null");
        if (slotIdx < 0) throw new IllegalArgumentException("slotIdx negatif");
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }

    public PageId getPageId() { return pageId; }
    public int getSlotIdx() { return slotIdx; }

    @Override
    public String toString() {
        return "RID(" + pageId.toString() + "," + slotIdx + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordId)) return false;
        RecordId other = (RecordId) o;
        return slotIdx == other.slotIdx && pageId.getFileIdx() == other.pageId.getFileIdx()
                && pageId.getPageIdx() == other.pageId.getPageIdx();
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId.getFileIdx(), pageId.getPageIdx(), slotIdx);
    }
}
