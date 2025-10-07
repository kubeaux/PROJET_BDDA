public class PageId {
    private int fileIdx;   // identifiant du fichier
    private int pageIdx;   // identifiant de la page dans ce fichier

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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        PageId other = (PageId) obj;
        return fileIdx == other.fileIdx && pageIdx == other.pageIdx;
    }

    @Override
    public int hashCode() {
        return 31 * fileIdx + pageIdx;
    }

    @Override
    public String toString() {
        return "PageId(" + fileIdx + ", " + pageIdx + ")";
    }
}
