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
    
    # ✅ CORRECTION : Ignorer les villes étrangères
    if "etranger" in ville.lower():
        return None
    
    # ✅ CORRECTION : Gestion spéciale pour Paris et arrondissements
    # On garde "Paris" directement sans chercher d'arrondissement spécifique
    if "Paris" in ville and "arrondissement" in ville:
        return "Paris"
    if ville == "Paris":
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
    
    return ville.strip()


def extraire_arrondissement_paris(ville_originale: str) -> str:
    """
    Extrait le numéro d'arrondissement de Paris depuis la ville originale
    Retourne le code postal correspondant (75001-75020)
    """
    if not ville_originale or pd.isna(ville_originale):
        return "75001"  # Par défaut
    
    ville_str = str(ville_originale)
    
    # Chercher "13e arrondissement" ou "14e arrondissement"
    import re
    
    # Pattern pour "13e arrondissement de Paris" ou "14e arrondissement"
    match = re.search(r'(\d+)e?\s*arrondissement', ville_str, re.IGNORECASE)
    if match:
        num_arr = int(match.group(1))
        # Convertir en code postal : arrondissement 1 = 75001, arrondissement 13 = 75013
        if 1 <= num_arr <= 20:
            return f"750{num_arr:02d}"  # Format 75001, 75013, etc.
    
    # Si c'est juste "Paris" sans arrondissement, retourner 75001 par défaut
    if "Paris" in ville_str:
        return "75001"
    
    return None


def get_commune_data_insee(ville: str, ville_originale: str = None) -> Dict:
    """
    Recupere les donnees geographiques et demographiques INSEE via l'API Geo.  
    
    Args:
        ville (str): Nom de la commune nettoyé
        ville_originale (str): Nom de la ville original (pour extraire l'arrondissement)
    
    Returns:
        Dict: Dictionnaire avec donnees INSEE ou {} si erreur
    """
    ville_clean = nettoyer_ville(ville)
    
    # ✅ CORRECTION : Si la ville est None (étranger ou invalide), retourner {}
    if not ville_clean:
        if ville and "etranger" in str(ville).lower():
            print(f"      [SKIP] Ville etrangere: '{ville}' - pas de recherche INSEE")
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
            print(f"      ATTENTION: Commune '{ville}' (nettoyee: '{ville_clean}') non trouvee")
            return {}
        
        commune = data[0]
        
        # Calculs
        surface_hectares = commune.get('surface', 0)
        surface_km2 = surface_hectares / 100
        population = commune.get('population', 0)
        densite = round(population / surface_km2, 2) if surface_km2 > 0 else 0
        
        # ✅ CORRECTION MAJEURE : Gestion spéciale pour Paris
        codes_postaux = commune.get('codesPostaux', [])
        
        if ville_clean == "Paris":
            # Pour Paris, extraire l'arrondissement de la ville originale
            if ville_originale:
                code_postal = extraire_arrondissement_paris(ville_originale)
            else:
                # Sinon, prendre les codes parisiens (75XXX) et le premier
                codes_paris = [cp for cp in codes_postaux if cp.startswith('75')]
                code_postal = codes_paris[0] if codes_paris else codes_postaux[0]
        else:
            # Pour les autres villes : un seul code postal
            code_postal = codes_postaux[0] if codes_postaux else None
        
        return {
            "commune_nom": commune.get('nom'),
            "commune_population": population,
            "commune_surface_km2": round(surface_km2, 2),
            "commune_densite": densite,
            "commune_departement": commune.get('codeDepartement'),
            "commune_region": commune.get('codeRegion'),
            "commune_code_postal": code_postal
        }
    
    except requests.exceptions.Timeout: 
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
    
    # 2. Initialiser les colonnes INSEE (SANS commune_code)
    insee_cols = [
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
    
    # 3. Récupérer les villes uniques (en filtrant les étrangers)
    villes_uniques = df['ville_naissance'].dropna().unique()
    villes_francaises = [v for v in villes_uniques if "etranger" not in str(v).lower()]
    villes_etrangeres = [v for v in villes_uniques if "etranger" in str(v).lower()]
    
    print(f"\n[ANALYSE] {len(villes_uniques)} villes de naissance uniques detectees")
    print(f"  - Villes francaises: {len(villes_francaises)}")
    print(f"  - Villes etrangeres (ignorees): {len(villes_etrangeres)}")
    
    if villes_etrangeres:
        print(f"\n[INFO] Villes etrangeres (pas de recherche INSEE):")
        for v in villes_etrangeres:
            print(f"  - {v}")
    
    # 4. Enrichir via API Geo INSEE (uniquement villes françaises)
    print(f"\n[ENRICHISSEMENT] Interrogation de l'API Geo INSEE...")
    print("-" * 70)
    
    cache_insee = {}
    
    for idx, ville in enumerate(villes_francaises, 1):
        ville_clean = nettoyer_ville(ville)
        print(f"   [{idx}/{len(villes_francaises)}] '{ville}' -> '{ville_clean}'")
        
        if ville not in cache_insee:
            # Passer la ville originale pour extraire l'arrondissement
            cache_insee[ville] = get_commune_data_insee(ville, ville_originale=ville)
            time.sleep(0.5)
    
    # Ajouter les villes étrangères au cache avec des valeurs vides
    for ville in villes_etrangeres:
        cache_insee[ville] = {}
    
    print("-" * 70)
    
    # 5. Appliquer les données au DataFrame
    print(f"\n[FUSION] Application des donnees INSEE au dataset...")
    
    for index, row in df.iterrows():
        ville = row['ville_naissance']
        if pd.notna(ville) and ville in cache_insee: 
            insee_data = cache_insee[ville]
            for key, value in insee_data.items():
                df.at[index, key] = value
    
    # 6. Statistiques
    nb_enrichis = df['commune_code_postal'].notna().sum()
    taux_succes = (nb_enrichis / len(df)) * 100
    nb_etrangers = len([v for v in df['ville_naissance'].dropna() if "etranger" in str(v).lower()])
    
    print(f"   SUCCES: {nb_enrichis}/{len(df)} joueurs enrichis ({taux_succes:.1f}%)")
    print(f"   IGNORE: {nb_etrangers} joueurs nes a l'etranger")
    
    # 7. Sauvegarde
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/joueurs_avec_insee.csv"
    
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
    if df['commune_population'].notna().any():
        print("\n" + "="*70)
        print("STATISTIQUES DESCRIPTIVES")
        print("="*70)
        print(f"   Population moyenne:      {df['commune_population'].mean():.0f} habitants")
        print(f"   Population mediane:      {df['commune_population'].median():.0f} habitants")
        print(f"   Population min:          {df['commune_population'].min():.0f} habitants")
        print(f"   Population max:          {df['commune_population'].max():.0f} habitants")
        print(f"   Densite moyenne:         {df['commune_densite'].mean():.0f} hab/km2")
        print(f"   Densite mediane:         {df['commune_densite'].median():.0f} hab/km2")
    
    # 10. Vérification spéciale pour Paris
    paris_joueurs = df[df['commune_nom'] == 'Paris']
    if len(paris_joueurs) > 0:
        print("\n" + "="*70)
        print("VERIFICATION PARIS")
        print("="*70)
        print(f"   Joueurs nes a Paris: {len(paris_joueurs)}")
        for _, joueur in paris_joueurs.iterrows():
            ville_orig = joueur.get('ville_naissance', 'N/A')
            cp = joueur.get('commune_code_postal', 'N/A')
            print(f"   - {joueur['nom']:25s} | Ville: {ville_orig:30s} | CP: {cp}")
    
    print("\n" + "="*70)
    print("ENRICHISSEMENT INSEE TERMINE")
    print("="*70)


if __name__ == "__main__":
    enrich_with_insee()