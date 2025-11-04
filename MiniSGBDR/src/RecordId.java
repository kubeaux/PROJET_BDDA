
public class RecordId {
    private PageId pageId; 
    private int slotIdx;

    public RecordId(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }

    public PageId getPageId() {
        return pageId;
    }

    public int getSlotIdx() {
        return slotIdx;
    }

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public void setSlotIdx(int slotIdx) {
        this.slotIdx = slotIdx;
    }

    @Override
    public String toString() {
        return "RecordId{pageId=" + pageId + ", slotIdx=" + slotIdx + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RecordId)) return false;
        RecordId other = (RecordId) obj;
        return this.pageId.equals(other.pageId) && this.slotIdx == other.slotIdx;
    }

    @Override
    public int hashCode() {
        return pageId.hashCode() * 31 + slotIdx;
    }
}
