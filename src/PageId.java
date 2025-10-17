package minisgbd;

public class PageId{
    private int fileIdx;
    private int  pageIdx;

    public pageId(int fileIdx, int pageIdx){
        this.fileIdx = fileIdx;
        this.pageIdx = pageIdx;
    }

    // Getters 
    public int getFileIdx(){
        return fileIdx;
    }

    public int getPageIdx(){
        return pageIdx;
    }

    @Override
    public String toString(){
        return "P[" + fileIdx + ":" + pageIdx + "]";
    }

}