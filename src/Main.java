package minigbd;

// D. Application Console
public class Main {
    public static void main(String[] args) {
        
        // 1. Exécutez les tests (Section G)
        DBConfigTest.runTests();
        
        // 2. Démarrez l'application console (Section D)
        System.out.println("\n--- Démarrage du MiniSGBDR ---");
        
        try {
            // Test de chargement avec votre fichier config.txt
            DBConfig initialConfig = DBConfig.LoadDBConfig("config.txt");
            System.out.println("Configuration chargée avec succès: " + initialConfig);
        } catch (Exception e) {
            System.err.println("Erreur fatale lors du chargement de la configuration initiale: " + e.getMessage());
            // System.exit(1); // En cas de production, on arrêterait ici.
        }
        
        // Boucle de commande (sera implémentée dans les prochains TPs)
        System.out.println("\n(La boucle de commandes EXIT sera implémentée plus tard.)");
        
    }
}