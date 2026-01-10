import pandas as pd
import os
import sys
import time

# --- GESTION DES IMPORTS ET DU "CONTRAT" AVEC L'ÉQUIPE ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) # Remonte à /src
sys.path.append(parent_dir)

try:
    # TODO POUR LES AUTRES MEMBRES DE L'ÉQUIPE :
    # 1. Le fichier 'src/ingestion/get_sofifa.py' doit exister.
    # 2. Il doit contenir une fonction nommée exactement 'get_sofifa_real_stats'.
    from ingestion.get_sofifa_data import get_sofifa_real_stats

    # TODO POUR LES AUTRES MEMBRES DE L'ÉQUIPE :
    # 1. Le fichier 'src/ingestion/get_transfermarkt.py' doit exister.
    # 2. Il doit contenir une fonction nommée exactement 'get_transfermarkt_real_value'.
    from ingestion.get_transfermarkt import get_transfermarkt_real_value

except ImportError as e:
    print("\n❌ ERREUR D'IMPORT CRITIQUE :")
    print(f"Impossible de trouver les fichiers ou les fonctions attendues dans 'src/ingestion/'.")
    print(f"Détail de l'erreur : {e}")
    print("👉 Vérifiez que vous avez bien créé les fichiers et nommé les fonctions comme convenu.")
    sys.exit(1)


def main():
    print("🚀 Démarrage du pipeline de fusion...")

    # A. Chargement de la liste des joueurs de l'équipe de France
    path_base = os.path.join(parent_dir, "..", "data", "raw", "joueurs_base.csv")
    
    if not os.path.exists(path_base):
        print(f"❌ Erreur : Le fichier {path_base} n'existe pas.")
        print("👉 Lance d'abord 'python src/ingestion/get_players.py'")
        return

    df_base = pd.read_csv(path_base)
    total_joueurs = len(df_base)
    print(f"📋 {total_joueurs} joueurs chargés à enrichir.")

    enrichissements = []

    # B. Boucle d'enrichissement
    for index, row in df_base.iterrows():
        nom = row['nom']
        dob = row['date_naissance'] 
        
        print(f"[{index+1}/{total_joueurs}] Traitement de : {nom}...")

        # --- Appel Source 2 : SoFIFA ---
        try:
            # TODO : La fonction 'get_sofifa_real_stats' doit :
            # - Accepter (nom, date_naissance)
            # - Renvoyer un dictionnaire (ex: {'sofifa_rating': 85, 'vitesse': 80})
            # - Renvoyer {} ou None si le joueur n'est pas trouvé
            stats_fifa = get_sofifa_real_stats(nom, dob)
            
            if stats_fifa is None: 
                stats_fifa = {}
        except Exception as e:
            print(f"   ⚠️ Erreur script SoFIFA : {e}")
            stats_fifa = {}

        # --- Appel Source 3 : Transfermarkt ---
        try:
            # TODO : La fonction 'get_transfermarkt_real_value' doit :
            # - Accepter (nom)
            # - Renvoyer un dictionnaire (ex: {'valeur_euro': 15000000, 'club': 'PSG'})
            # - Renvoyer {} ou None si le joueur n'est pas trouvé
            data_tm = get_transfermarkt_real_value(nom)
            
            if data_tm is None: 
                data_tm = {}
        except Exception as e:
            print(f"   ⚠️ Erreur script Transfermarkt : {e}")
            data_tm = {}

        # --- Assemblage de la ligne ---
        ligne_complete = {
            "wikidata_id": row['wikidata_id'], # La clé commune (NE PAS TOUCHER)
            **stats_fifa,                      # Fusionne les notes FIFA
            **data_tm                          # Fusionne les prix TM
        }
        enrichissements.append(ligne_complete)

        # PAUSE RATE LIMIT (SoFIFA limité à 60req/min)
        time.sleep(1.5) 

    # C. Création du fichier final
    df_extra = pd.DataFrame(enrichissements)
    
    # Jointure Gauche pour ne perdre aucun joueur de la liste officielle
    df_final = pd.merge(df_base, df_extra, on="wikidata_id", how="left")

    output_path = os.path.join(parent_dir, "..", "data", "processed", "dataset_final.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_final.to_csv(output_path, index=False)
    
    print(f"✅ SUCCÈS ! Fichier généré : {output_path}")
    print("Aperçu des données :")
    print(df_final.head())

if __name__ == "__main__":
    main()