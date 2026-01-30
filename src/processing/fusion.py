import pandas as pd
import os
import sys

# --- GESTION DES IMPORTS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # Remonte à /src
sys.path.append(parent_dir)


def main():
    print("="*70)
    print("PIPELINE DE FUSION - Dataset Equipe de France")
    print("="*70)

    # A. Chargement de toutes les sources de données disponibles
    
    # 1. Données de base (Wikipedia)
    path_base = os.path.join(parent_dir, "..", "data", "raw", "joueurs_base.csv")
    if not os.path.exists(path_base):
        print(f"[ERREUR] Le fichier {path_base} n'existe pas.")
        print("-> Lance d'abord 'python src/ingestion/get_players.py'")
        return

    df_base = pd.read_csv(path_base)
    print(f"\n[1/4] OK Donnees Wikipedia chargees : {len(df_base)} joueurs")
    print(f"      Colonnes: {', '.join(df_base.columns)}")

    # 2. Données Wikidata
    path_wikidata = os.path.join(parent_dir, "..", "data", "processed", "joueurs_enrichis.csv")
    if os.path.exists(path_wikidata):
        df_wikidata = pd.read_csv(path_wikidata)
        print(f"\n[2/4] OK Donnees Wikidata chargees : {len(df_wikidata)} joueurs")
        print(f"      Colonnes ajoutees: {', '.join([c for c in df_wikidata.columns if c not in df_base.columns])}")
    else:
        print(f"\n[2/4] ATTENTION Donnees Wikidata non trouvees (fichier: {path_wikidata})")
        print("      -> Lance 'python src/ingestion/get_wikidata_data.py'")
        df_wikidata = None

    # 3. Données INSEE
    path_insee = os.path.join(parent_dir, "..", "data", "processed", "joueurs_avec_insee.csv")
    if os.path.exists(path_insee):
        df_insee = pd.read_csv(path_insee)
        print(f"\n[3/4] OK Donnees INSEE chargees : {len(df_insee)} joueurs")
        
        # Compter les colonnes INSEE ajoutées
        if df_wikidata is not None:
            nouvelles_cols = [c for c in df_insee.columns if c not in df_wikidata.columns]
        else:
            nouvelles_cols = [c for c in df_insee.columns if c not in df_base.columns]
        print(f"      Colonnes ajoutees: {', '.join(nouvelles_cols)}")
    else:
        print(f"\n[3/4] ATTENTION Donnees INSEE non trouvees (fichier: {path_insee})")
        print("      -> Lance 'python src/ingestion/get_insee_data.py'")
        df_insee = None

    # 4. Données Équipements sportifs
    path_equipements = os.path.join(parent_dir, "..", "data", "final", "joueurs_complet.csv")
    if os.path.exists(path_equipements):
        df_equipements = pd.read_csv(path_equipements)
        print(f"\n[4/4] OK Donnees Equipements chargees : {len(df_equipements)} joueurs")
        
        # Compter les colonnes Équipements ajoutées
        if df_insee is not None:
            nouvelles_cols = [c for c in df_equipements.columns if c not in df_insee.columns]
        else:
            nouvelles_cols = []
        if nouvelles_cols:
            print(f"      Colonnes ajoutees: {', '.join(nouvelles_cols)}")
    else:
        print(f"\n[4/4] ATTENTION Donnees Equipements non trouvees (fichier: {path_equipements})")
        print("      -> Lance 'python src/ingestion/get_equipements_data.py'")
        df_equipements = None

    # B. Fusion progressive des données
    print("\n" + "="*70)
    print("FUSION DES DONNEES")
    print("="*70)

    # Commencer avec la source la plus complète disponible
    if df_equipements is not None:
        df_final = df_equipements
        print("[OK] Utilisation du dataset complet (avec equipements)")
    elif df_insee is not None:
        df_final = df_insee
        print("[OK] Utilisation du dataset avec INSEE")
    elif df_wikidata is not None:
        df_final = df_wikidata
        print("[OK] Utilisation du dataset avec Wikidata")
    else:
        df_final = df_base
        print("[ATTENTION] Utilisation des donnees de base uniquement (Wikipedia)")

    # C. Statistiques du dataset final
    print("\n" + "="*70)
    print("STATISTIQUES DU DATASET FINAL")
    print("="*70)
    
    print(f"\nNombre de joueurs: {len(df_final)}")
    print(f"Nombre de colonnes: {len(df_final.columns)}")
    
    print("\nCompletude par colonne:")
    for col in df_final.columns:
        non_null = df_final[col].notna().sum()
        pct = (non_null / len(df_final)) * 100
        if pct == 100:
            status = "[OK]"
        elif pct >= 80:
            status = "[  ]"
        else:
            status = "[!!]"
        print(f"  {status} {col:30s}: {non_null:2d}/{len(df_final)} ({pct:5.1f}%)")

    # Calcul du taux de complétude global
    total_cells = len(df_final) * len(df_final.columns)
    filled_cells = df_final.notna().sum().sum()
    taux_global = (filled_cells / total_cells) * 100
    
    print(f"\nTaux de completude global: {taux_global:.1f}%")
    if taux_global >= 90:
        print("   >>> EXCELLENT")
    elif taux_global >= 75:
        print("   >>> TRES BON")
    elif taux_global >= 60:
        print("   >>> BON")
    else:
        print("   >>> A AMELIORER")

    # D. Sauvegarde du dataset final
    output_path = os.path.join(parent_dir, "..", "data", "final", "dataset_final.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*70)
    print("SUCCES !")
    print("="*70)
    print(f"Fichier genere: {output_path}")
    print(f"\nApercu des donnees:")
    print("-" * 70)
    print(df_final.head(5).to_string())
    
    # E. Résumé des sources utilisées
    print("\n" + "="*70)
    print("SOURCES DE DONNEES UTILISEES")
    print("="*70)
    sources = []
    if df_base is not None:
        sources.append("[OK] Wikipedia (liste des joueurs)")
    if df_wikidata is not None:
        sources.append("[OK] Wikidata (taille, ville de naissance)")
    if df_insee is not None:
        sources.append("[OK] INSEE (demographie, geographie)")
    if df_equipements is not None:
        sources.append("[OK] Equipements Sportifs (infrastructures)")
    
    for source in sources:
        print(f"  {source}")
    
    print("\n" + "="*70)
    print("PIPELINE TERMINE")
    print("="*70)


if __name__ == "__main__":
    main()