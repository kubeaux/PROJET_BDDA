package minisgbd;

public class PageId {
    public int fileIdx;
    public int pageIdx;

    public PageId(int fileIdx, int pageIdx) {
        this.fileIdx = fileIdx;
        this.pageIdx = pageIdx;
    }

    public int getFileIdx() { 
        return fileIdx; 
    }

    public int getPageIdx() { 
        return pageIdx; 
    }

    @Override
    public String toString() {
        return "[File " + fileIdx + ", Page " + pageIdx + "]";
    }
}
