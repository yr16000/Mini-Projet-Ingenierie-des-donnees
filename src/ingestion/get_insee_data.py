import pandas as pd
import requests
import os
import time
from typing import Dict


def nettoyer_ville(ville: str) -> str:
    """Normalise les noms de villes pour l'API Geo"""
    if not ville or pd.isna(ville):
        return None
    
    ville = str(ville)
    
    # ✅ CAS PARTICULIERS : Arrondissements parisiens
    if "arrondissement de Paris" in ville or "Paris" in ville:
        return "Paris"
    
    # Autres grandes villes avec arrondissements
    if "arrondissement de Lyon" in ville or ville.startswith("Lyon"):
        return "Lyon"
    if "arrondissement de Marseille" in ville or ville.startswith("Marseille"):
        return "Marseille"
    
    # Supprimer parenthèses et chiffres d'arrondissements
    ville = ville.split("(")[0].strip()
    ville = ville.replace(" 1er", "").replace(" 2e", "").replace(" 3e", "")
    ville = ville.replace(" 4e", "").replace(" 5e", "").replace(" 6e", "")
    ville = ville.replace(" 7e", "").replace(" 8e", "").replace(" 9e", "")
    for i in range(10, 21):
        ville = ville.replace(f" {i}e", "")
    
    return ville. strip()


def get_commune_data_insee(ville: str) -> Dict:
    """
    Recupere les donnees geographiques et demographiques INSEE via l'API Geo.  
    
    Args:
        ville (str): Nom de la commune
    
    Returns:
        Dict:  Dictionnaire avec donnees INSEE ou {} si erreur
    """
    ville_clean = nettoyer_ville(ville)
    
    if not ville_clean:
        return {}
    
    try: 
        url = "https://geo.api.gouv.fr/communes"
        params = {
            "nom": ville_clean,
            "fields": "nom,code,population,surface,codeDepartement,codeRegion,codesPostaux",
            "limit": 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            print(f"      ATTENTION:  Commune '{ville}' (nettoyee:  '{ville_clean}') non trouvee")
            return {}
        
        commune = data[0]
        
        # Calculs
        surface_hectares = commune.  get('surface', 0)
        surface_km2 = surface_hectares / 100
        population = commune. get('population', 0)
        densite = round(population / surface_km2, 2) if surface_km2 > 0 else 0
        
        # ✅ CORRECTION : Pour Paris/Lyon/Marseille, prendre le premier code postal
        codes_postaux = commune. get('codesPostaux', [])
        code_postal = codes_postaux[0] if codes_postaux else None
        
        return {
            "commune_code": commune.get('code'),
            "commune_nom": commune.get('nom'),
            "commune_population": population,
            "commune_surface_km2": round(surface_km2, 2),
            "commune_densite": densite,
            "commune_departement": commune.get('codeDepartement'),
            "commune_region": commune.get('codeRegion'),
            "commune_code_postal": code_postal
        }
    
    except requests.exceptions.  Timeout: 
        print(f"      TIMEOUT API Geo pour '{ville}'")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"      ERREUR API Geo pour '{ville}': {e}")
        return {}
    except Exception as e:
        print(f"      ERREUR traitement pour '{ville}': {e}")
        return {}


def enrich_with_insee():
    """
    Enrichit le dataset avec les donnees INSEE (demographie, geographie)
    """
    print("="*70)
    print("ENRICHISSEMENT INSEE - Donnees demographiques et geographiques")
    print("="*70)
    
    # 1. Charger le fichier enrichi Wikidata
    input_path = "data/processed/joueurs_enrichis.csv"
    
    if not os.path.exists(input_path):
        print(f"\nERREUR: Fichier '{input_path}' introuvable.")
        print("Veuillez d'abord executer 'get_wikidata_data.py'")
        return
    
    df = pd.read_csv(input_path)
    print(f"\n[CHARGEMENT] {len(df)} joueurs charges depuis {input_path}")
    
    # 2. Initialiser les colonnes INSEE
    insee_cols = [
        "commune_code",
        "commune_nom",
        "commune_population",
        "commune_surface_km2",
        "commune_densite",
        "commune_departement",
        "commune_region",
        "commune_code_postal"
    ]
    
    for col in insee_cols:
        df[col] = None
    
    # 3. Récupérer les villes uniques
    villes_uniques = df['ville_naissance'].dropna().unique()
    print(f"\n[ANALYSE] {len(villes_uniques)} villes de naissance uniques detectees")
    
    # 4. Enrichir via API Geo INSEE
    print(f"\n[ENRICHISSEMENT] Interrogation de l'API Geo INSEE...")
    print("-" * 70)
    
    cache_insee = {}
    
    for idx, ville in enumerate(villes_uniques, 1):
        ville_clean = nettoyer_ville(ville)
        print(f"   [{idx}/{len(villes_uniques)}] '{ville}' -> '{ville_clean}'")
        
        if ville not in cache_insee:
            cache_insee[ville] = get_commune_data_insee(ville)
            time.sleep(0.5)
    
    print("-" * 70)
    
    # 5. Appliquer les données au DataFrame
    print(f"\n[FUSION] Application des donnees INSEE au dataset...")
    
    for index, row in df.iterrows():
        ville = row['ville_naissance']
        if pd.notna(ville) and ville in cache_insee: 
            insee_data = cache_insee[ville]
            for key, value in insee_data.  items():
                df.at[index, key] = value
    
    # 6. Statistiques
    nb_enrichis = df['commune_code'].notna().sum()
    taux_succes = (nb_enrichis / len(df)) * 100
    
    print(f"   SUCCES: {nb_enrichis}/{len(df)} joueurs enrichis ({taux_succes:.1f}%)")
    
    # 7. Sauvegarde avec forçage du type string pour commune_code
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/joueurs_avec_insee.csv"
    
    # ✅ Forcer commune_code en string pour éviter les problèmes de float
    df['commune_code'] = df['commune_code'].astype(str).replace('nan', '')
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n[SAUVEGARDE] Fichier genere: {output_path}")
    
        # 8. Résumé des colonnes
    print("\n" + "="*70)
    print("RAPPORT DE COMPLETUDE - DONNEES INSEE")
    print("="*70)
    
    for col in insee_cols: 
        non_null = df[col].notna().sum()
        pct = (non_null / len(df)) * 100
        print(f"   {col:25}: {non_null:5d} / {len(df):5d} ({pct:5.1f}%)")   
    
    # 9. Statistiques descriptives
    if df['commune_population']. notna().any():
        print("\n" + "="*70)
        print("STATISTIQUES DESCRIPTIVES")
        print("="*70)
        print(f"   Population moyenne:      {df['commune_population'].mean():.0f} habitants")
        print(f"   Population mediane:      {df['commune_population'].median():.0f} habitants")
        print(f"   Population min:          {df['commune_population'].min():.0f} habitants")
        print(f"   Population max:          {df['commune_population'].max():.0f} habitants")
        print(f"   Densite moyenne:         {df['commune_densite'].mean():.0f} hab/km2")
        print(f"   Densite mediane:         {df['commune_densite']. median():.0f} hab/km2")
    
    print("\n" + "="*70)
    print("ENRICHISSEMENT INSEE TERMINE")
    print("="*70)


if __name__ == "__main__":
    enrich_with_insee()