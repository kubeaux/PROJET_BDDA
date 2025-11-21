package minisgbd;

import java.io.IOException;
import java.util.*;

public class BufferManager {

    public enum Policy { LRU, MRU }

    private DBConfig config;
    private DiskManager diskManager;

    // Structure interne pour représenter un buffer
    private class Buffer {
        PageId pid;
        byte[] data;
        int pinCount;
        boolean dirty;

        Buffer() {
            this.pid = null;
            this.data = new byte[config.getPagesize()];
            this.pinCount = 0;
            this.dirty = false;
        }
    }

    private final Buffer[] buffers;
    private Policy currentPolicy;

    // Liste pour gérer LRU/MRU (indices dans buffers)
    private final LinkedList<Integer> usageList;

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.buffers = new Buffer[config.getBm_buffercount()];
        for (int i = 0; i < buffers.length; i++) buffers[i] = new Buffer();
        this.currentPolicy = config.getBm_policy().equalsIgnoreCase("MRU") ? Policy.MRU : Policy.LRU;
        this.usageList = new LinkedList<>();
    }

    public DBConfig getConfig() {
        return config;
    }


    /** Retourne le buffer contenant la page demandée, ou charge une page libre en appliquant la politique de remplacement */
    public synchronized byte[] GetPage(PageId pid) throws IOException {
        // Chercher si la page est déjà dans un buffer
        for (int i = 0; i < buffers.length; i++) {
            Buffer buf = buffers[i];
            if (buf.pid != null && buf.pid.getFileIdx() == pid.getFileIdx() && buf.pid.getPageIdx() == pid.getPageIdx()) {
                buf.pinCount++;
                updateUsageList(i);
                return buf.data;
            }
        }

        // Trouver un buffer libre ou appliquer politique de remplacement
        int bufIdx = -1;
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i].pinCount == 0 && buffers[i].pid == null) {
                bufIdx = i;
                break;
            }
        }

        if (bufIdx == -1) { // Pas de buffer vide, appliquer LRU/MRU
            bufIdx = selectVictimBuffer();
            Buffer victim = buffers[bufIdx];
            if (victim.dirty && victim.pid != null) {
                diskManager.WritePage(victim.pid, victim.data);
            }
            victim.pid = null; // libération
        }

        // Charger la page demandée depuis le DiskManager
        Buffer buf = buffers[bufIdx];
        diskManager.ReadPage(pid, buf.data);
        buf.pid = pid;
        buf.pinCount = 1;
        buf.dirty = false;
        updateUsageList(bufIdx);

        return buf.data;
    }

    /** Libérer une page et mettre à jour dirty flag */
    public synchronized void FreePage(PageId pid, boolean valDirty) {
        for (int i = 0; i < buffers.length; i++) {
            Buffer buf = buffers[i];
            if (buf.pid != null && buf.pid.getFileIdx() == pid.getFileIdx() && buf.pid.getPageIdx() == pid.getPageIdx()) {
                buf.pinCount = Math.max(0, buf.pinCount - 1);
                if (valDirty) buf.dirty = true;
                return;
            }
        }
    }

    /** Changer la politique de remplacement en cours */
    public synchronized void SetCurrentReplacementPolicy(String policy) {
        if (policy.equalsIgnoreCase("LRU")) currentPolicy = Policy.LRU;
        else if (policy.equalsIgnoreCase("MRU")) currentPolicy = Policy.MRU;
        else throw new IllegalArgumentException("Policy invalide : " + policy);
    }

    /** Écriture de toutes les pages dirty sur disque et réinitialisation des buffers */
    public synchronized void FlushBuffers() throws IOException {
        for (Buffer buf : buffers) {
            if (buf.dirty && buf.pid != null) {
                diskManager.WritePage(buf.pid, buf.data);
            }
            buf.pid = null;
            buf.pinCount = 0;
            buf.dirty = false;
        }
        usageList.clear();
    }

    /** Met à jour l'ordre d'utilisation pour LRU/MRU */
    private void updateUsageList(int bufIdx) {
        usageList.remove((Integer) bufIdx);
        if (currentPolicy == Policy.LRU) usageList.addLast(bufIdx);
        else usageList.addFirst(bufIdx);
    }

    /** Sélectionne un buffer à remplacer selon LRU ou MRU */
    private int selectVictimBuffer() {
        for (int idx : usageList) {
            if (buffers[idx].pinCount == 0) return idx;
        }
        // Si tous les buffers sont encore pinés, erreur
        throw new RuntimeException("Aucun buffer disponible pour remplacement !");
    }
}
