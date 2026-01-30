import pandas as pd
import requests
import os
import time
from typing import Dict


def download_equipements_dataset():
    """
    Telecharge le dataset complet des equipements sportifs depuis l'API
    """
    print("\n[TELECHARGEMENT] Dataset equipements sportifs depuis l'API...")
    
    dataset_path = "data/external/equipements_sportifs.csv"
    
    # Si déjà téléchargé, on ne le refait pas
    if os. path.exists(dataset_path):
        print(f"   Fichier deja present: {dataset_path}")
        return dataset_path
    
    try: 
        # URL de l'export CSV complet
        url = "https://equipements.sports.gouv.fr/api/explore/v2.1/catalog/datasets/data-es/exports/csv"
        
        print(f"   Telechargement depuis: {url}")
        print("   Cela peut prendre 2-3 minutes (fichier volumineux)...")
        
        os.makedirs("data/external", exist_ok=True)
        
        # Télécharger le fichier (avec délimiteur ; souvent utilisé par l'API française)
        response = requests.get(url, timeout=180, stream=True)
        response.raise_for_status()
        
        # Sauvegarder
        with open(dataset_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
        print(f"   SUCCES:  Fichier sauvegarde dans {dataset_path}")
        print(f"   Taille: {file_size_mb:.1f} MB")
        
        return dataset_path
    
    except requests.exceptions. Timeout:
        print("   ERREUR: Timeout lors du telechargement")
        print("   Conseil: Verifiez votre connexion ou telechargez manuellement")
        return None
    except Exception as e:
        print(f"   ERREUR: {e}")
        return None


def load_equipements_data():
    """
    Charge le dataset des equipements sportifs en memoire
    """
    print("\n[CHARGEMENT] Lecture du fichier equipements sportifs...")
    
    dataset_path = "data/external/equipements_sportifs.csv"
    
    if not os.path.exists(dataset_path):
        print(f"   ERREUR: Fichier non trouve: {dataset_path}")
        return None
    
    try:
        # L'API utilise souvent le séparateur ; au lieu de ,
        # On teste d'abord avec ;
        try:
            df = pd.read_csv(
                dataset_path,
                sep=';',
                dtype={'com_code': str},
                low_memory=False,
                encoding='utf-8'
            )
        except: 
            # Si ça échoue, on essaie avec ,
            df = pd.read_csv(
                dataset_path,
                sep=',',
                dtype={'com_code': str},
                low_memory=False,
                encoding='utf-8'
            )
        
        print(f"   SUCCES: {len(df)} equipements charges")
        print(f"   Colonnes disponibles: {len(df.columns)}")
        
        # Afficher les 5 premières colonnes pour diagnostic
        print(f"   Echantillon colonnes: {', '.join(df.columns[: 5]. tolist())}")
        
        return df
    
    except Exception as e: 
        print(f"   ERREUR lors du chargement: {e}")
        return None


def get_equipements_from_df(df_equipements: pd.DataFrame, code_commune: str, nom_ville: str) -> Dict:
    """
    Compte les equipements sportifs d'une commune depuis le DataFrame
    
    Args:
        df_equipements: DataFrame des equipements sportifs
        code_commune: Code INSEE de la commune
        nom_ville: Nom de la ville (pour affichage)
    
    Returns:
        Dict avec nb_equipements_sportifs et nb_terrains_football
    """
    if df_equipements is None or code_commune is None or pd.isna(code_commune):
        return {
            "nb_equipements_sportifs": None,
            "nb_terrains_football": None
        }
    
    # Nettoyer le code commune (retirer . 0 si présent)
    try:
        code_commune_clean = str(int(float(code_commune)))
    except:
        code_commune_clean = str(code_commune)
    
    try:
        # Filtrer les équipements de la commune
        equipements_commune = df_equipements[df_equipements['com_code'] == code_commune_clean]
        
        total_equipements = len(equipements_commune)
        
        # Compter les terrains de football
        # Chercher dans la colonne 'act_lib' (Libellé de l'activité)
        if 'act_lib' in df_equipements.columns:
            terrains_football = equipements_commune[
                equipements_commune['act_lib']. str.contains('Football|Futsal', case=False, na=False)
            ]
            total_foot = len(terrains_football)
        else:
            # Fallback: chercher dans toutes les colonnes texte
            total_foot = 0
            for col in df_equipements.select_dtypes(include=['object']).columns:
                if equipements_commune[col].str.contains('Football|Futsal', case=False, na=False).any():
                    total_foot = equipements_commune[col].str. contains('Football|Futsal', case=False, na=False).sum()
                    break
        
        print(f"      -> {total_equipements} equipements | {total_foot} terrains foot")
        
        return {
            "nb_equipements_sportifs": total_equipements,
            "nb_terrains_football": total_foot
        }
    
    except Exception as e: 
        print(f"      ERREUR pour {nom_ville}: {e}")
        return {
            "nb_equipements_sportifs": None,
            "nb_terrains_football": None
        }


def enrich_with_equipements():
    """
    Enrichit le dataset avec les donnees d'equipements sportifs
    """
    print("="*70)
    print("ENRICHISSEMENT EQUIPEMENTS SPORTIFS - API Sports Gouv")
    print("="*70)
    
    # 1. Charger le fichier enrichi INSEE
    input_path = "data/processed/joueurs_avec_insee.csv"
    
    if not os.path.exists(input_path):
        print(f"\nERREUR: Fichier '{input_path}' introuvable.")
        print("Veuillez d'abord executer 'get_insee_data.py'")
        return
    
    df = pd.read_csv(input_path, dtype={'commune_code': str})
    print(f"\n[CHARGEMENT] {len(df)} joueurs charges depuis {input_path}")
    
    # 2. Vérifier que les codes communes existent
    nb_codes = df['commune_code'].notna().sum()
    if nb_codes == 0:
        print("\nERREUR: Aucun code commune trouve dans le dataset.")
        print("Impossible d'enrichir sans codes INSEE.")
        return
    
    print(f"[ANALYSE] {nb_codes} codes communes disponibles")
    
    # 3. Télécharger le dataset des équipements si nécessaire
    dataset_path = download_equipements_dataset()
    
    if dataset_path is None:
        print("\nERREUR: Impossible de continuer sans le dataset equipements")
        return
    
    # 4. Charger le dataset des équipements
    df_equipements = load_equipements_data()
    
    if df_equipements is None:
        print("\nERREUR: Impossible de charger le dataset equipements")
        return
    
    # 5. Initialiser les colonnes
    df["nb_equipements_sportifs"] = None
    df["nb_terrains_football"] = None
    
    # 6. Récupérer les codes uniques
    codes_uniques = df['commune_code'].dropna().unique()
    print(f"\n[ENRICHISSEMENT] {len(codes_uniques)} communes uniques a traiter")
    print("-" * 70)
    
    cache_equipements = {}
    
    for idx, code in enumerate(codes_uniques, 1):
        # Récupérer le nom de la ville pour l'affichage
        nom_ville = df[df['commune_code'] == code]['commune_nom'].iloc[0]
        
        print(f"   [{idx}/{len(codes_uniques)}] {nom_ville} ({code})", end=" ")
        
        if code not in cache_equipements:
            cache_equipements[code] = get_equipements_from_df(df_equipements, code, nom_ville)
        else:
            print("      (cache)")
    
    print("-" * 70)
    
    # 7. Appliquer les données au DataFrame
    print(f"\n[FUSION] Application des donnees equipements au dataset...")
    
    for index, row in df.iterrows():
        code = row['commune_code']
        if pd.notna(code) and code in cache_equipements:
            equip_data = cache_equipements[code]
            df.at[index, "nb_equipements_sportifs"] = equip_data. get("nb_equipements_sportifs")
            df.at[index, "nb_terrains_football"] = equip_data.get("nb_terrains_football")
    
    # 8. Statistiques
    nb_enrichis = df['nb_equipements_sportifs'].notna().sum()
    taux_succes = (nb_enrichis / len(df)) * 100
    
    print(f"   SUCCES: {nb_enrichis}/{len(df)} joueurs enrichis ({taux_succes:.1f}%)")
    
    # 9. Sauvegarde
    os.makedirs("data/final", exist_ok=True)
    output_path = "data/final/joueurs_complet.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n[SAUVEGARDE] Fichier final genere: {output_path}")
    
    # 10. Résumé des colonnes
    print("\n" + "="*70)
    print("RAPPORT DE COMPLETUDE - DONNEES EQUIPEMENTS")
    print("="*70)
    
    equip_cols = ["nb_equipements_sportifs", "nb_terrains_football"]
    
    for col in equip_cols:
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"   {col: 30s}: {non_null:3d}/{len(df)} ({pct: 5.1f}%)")
    
    # 11. Statistiques descriptives
    if df['nb_equipements_sportifs']. notna().any():
        print("\n" + "="*70)
        print("STATISTIQUES DESCRIPTIVES")
        print("="*70)
        print(f"   Equipements sportifs moyen:   {df['nb_equipements_sportifs'].mean():.1f}")
        print(f"   Equipements sportifs median: {df['nb_equipements_sportifs'].median():.0f}")
        print(f"   Equipements sportifs min:     {df['nb_equipements_sportifs'].min():.0f}")
        print(f"   Equipements sportifs max:     {df['nb_equipements_sportifs'].max():.0f}")
        print(f"   Terrains football moyen:     {df['nb_terrains_football'].mean():.1f}")
        print(f"   Terrains football median:    {df['nb_terrains_football'].median():.0f}")
    
    # 12. Résumé final complet
    print("\n" + "="*70)
    print("DATASET FINAL COMPLET")
    print("="*70)
    print(f"   Fichier:   {output_path}")
    print(f"   Lignes:   {len(df)}")
    print(f"   Colonnes: {len(df.columns)}")
    
    print("\n   Liste des colonnes:")
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"      - {col: 30s}: {pct:5.1f}% rempli")
    
    print("\n" + "="*70)
    print("ENRICHISSEMENT EQUIPEMENTS TERMINE")
    print("="*70)


if __name__ == "__main__":
    enrich_with_equipements()