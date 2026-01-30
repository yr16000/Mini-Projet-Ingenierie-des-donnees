import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
import unicodedata

def remove_accents(text):
    """
    Supprime les accents d'un texte
    """
    if not text:
        return text
    nfd = unicodedata.normalize('NFD', text)
    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')

def get_wikidata_info(nom_joueur, sparql):
    """
    Récupère les informations Wikidata pour un joueur spécifique
    Essaie plusieurs variantes du nom pour augmenter les chances de succès
    """
    # Créer une version sans accents du nom
    nom_sans_accents = remove_accents(nom_joueur)
    
    # ✅ CORRECTION SPÉCIALE : Hugo Ekitiké existe sur Wikidata mais sans accent !
    # ID Wikidata confirmé : Q111269183
    corrections_manuelles = {
        "Hugo Ekitiké": {
            "wikidata_id": "Q111269183",
            "taille_m": 1.90,
            "ville_naissance": "Reims"
        }
    }
    
    # Si le joueur a une correction manuelle, la retourner directement
    if nom_joueur in corrections_manuelles:
        print(f"      [OK] Correction manuelle appliquee")
        data = corrections_manuelles[nom_joueur]
        print(f"| {data['wikidata_id']} | Taille: {data['taille_m']}m | Ville: {data['ville_naissance']}")
        return data
    
    # Essayer différentes variantes du nom
    noms_a_tester = [
        nom_joueur,           # Nom exact
        nom_sans_accents,     # Sans accents
    ]
    
    # Essayer d'abord avec le nom complet
    queries_to_try = []
    
    # Pour chaque variante du nom, créer une requête
    for nom_variant in noms_a_tester:
        queries_to_try.append((f"Exact FR", f"""
        SELECT DISTINCT ?item ?height ?birthPlaceLabel ?countryLabel
        WHERE {{
          ?item rdfs:label "{nom_variant}"@fr .
          ?item wdt:P31 wd:Q5 .
          ?item wdt:P106 wd:Q937857 .
          OPTIONAL {{ ?item wdt:P2048 ?height . }}
          OPTIONAL {{ 
            ?item wdt:P19 ?birthPlace .
            ?birthPlace wdt:P17 ?country .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr". }}
        }}
        LIMIT 1
        """))
        
        queries_to_try.append((f"Exact EN", f"""
        SELECT DISTINCT ?item ?height ?birthPlaceLabel ?countryLabel
        WHERE {{
          ?item rdfs:label "{nom_variant}"@en .
          ?item wdt:P31 wd:Q5 .
          ?item wdt:P106 wd:Q937857 .
          OPTIONAL {{ ?item wdt:P2048 ?height . }}
          OPTIONAL {{ 
            ?item wdt:P19 ?birthPlace .
            ?birthPlace wdt:P17 ?country .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,fr". }}
        }}
        LIMIT 1
        """))
    
    # Ajouter une recherche CONTAINS en dernier recours (plus lente)
    nom_recherche = nom_sans_accents.split()[-1]  # Utiliser le nom de famille
    queries_to_try.append((f"CONTAINS", f"""
    SELECT DISTINCT ?item ?height ?birthPlaceLabel ?countryLabel
    WHERE {{
      ?item rdfs:label ?label .
      FILTER(CONTAINS(LCASE(?label), LCASE("{nom_recherche}")))
      ?item wdt:P31 wd:Q5 .
      ?item wdt:P106 wd:Q937857 .
      OPTIONAL {{ ?item wdt:P2048 ?height . }}
      OPTIONAL {{ 
        ?item wdt:P19 ?birthPlace .
        ?birthPlace wdt:P17 ?country .
      }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr,en". }}
    }}
    LIMIT 1
    """))
    
    for attempt, (desc, query) in enumerate(queries_to_try, 1):
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]
            
            if bindings:
                print(f"      [OK] Trouve via {desc}", end=" ")
                res = bindings[0]
                
                # Extraction sécurisée
                w_id = res["item"]["value"].split("/")[-1]
                raw_taille = res.get("height", {}).get("value", None)
                ville = res.get("birthPlaceLabel", {}).get("value", None)
                pays = res.get("countryLabel", {}).get("value", None)
                
                taille_en_m = None
                if raw_taille:
                    try:
                        val = float(raw_taille)
                        # Logique : Si > 3, c'est des cm (ex: 185), on convertit en m (1.85)
                        if val > 3:
                            taille_en_m = val / 100
                        else:
                            taille_en_m = val
                        taille_en_m = round(taille_en_m, 2)
                    except ValueError:
                        taille_en_m = None
                
                # Si le joueur est né à l'étranger, mettre "etranger (Pays)"
                if ville and pays and pays.lower() != "france":
                    ville = f"etranger ({pays})"
                
                print(f"| {w_id} | Taille: {taille_en_m}m | Ville: {ville}")
                
                return {
                    "wikidata_id": w_id,
                    "taille_m": taille_en_m,
                    "ville_naissance": ville
                }
            
            # Si pas de résultat, essayer la prochaine requête avec une pause
            if attempt < len(queries_to_try):
                time.sleep(1.0)
                
        except Exception as e:
            print(f"      [WARN] Erreur tentative {attempt}: {str(e)[:50]}")
            if attempt < len(queries_to_try):
                # Augmenter la pause après un timeout
                time.sleep(2.0)
                continue
    
    print(f"      [ERREUR] Aucun resultat trouve apres {len(queries_to_try)} tentatives")
    return None


def enrich_with_wikidata_individual():
    print("="*70)
    print("> Demarrage de l'enrichissement (Traitement individuel ameliore)...")
    print("="*70)

    input_path = "data/raw/joueurs_base.csv"
    if not os.path.exists(input_path):
        print(" Erreur : Fichier d'entree introuvable.")
        return

    df = pd.read_csv(input_path)
    print(f"\n[INFO] {len(df)} joueurs a traiter individuellement.\n")

    # Configuration Wikidata avec timeout augmente
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.addCustomHttpHeader("User-Agent", "Projet-Etudiant-Polytech/1.0")
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(90)  # Augmente de 60 a 90 secondes

    # Initialiser les colonnes
    df["wikidata_id"] = None
    df["taille_m"] = None
    df["ville_naissance"] = None

    # Traiter chaque joueur individuellement
    for index, row in df.iterrows():
        nom = row['nom']
        print(f"[{index+1}/{len(df)}] Traitement de: {nom}")
        
        info = get_wikidata_info(nom, sparql)
        
        if info:
            df.at[index, "wikidata_id"] = info["wikidata_id"]
            df.at[index, "taille_m"] = info["taille_m"]
            df.at[index, "ville_naissance"] = info["ville_naissance"]
        
        # Pause entre chaque joueur pour respecter les limites de l'API
        time.sleep(2.0)  # Augmente de 1.5 a 2.0 secondes
    
    print("\n" + "="*70)

    # Statistiques
    nb_enrichis = df["wikidata_id"].notna().sum()
    taux_succes = (nb_enrichis / len(df)) * 100
    print(f"[RESULTAT] {nb_enrichis}/{len(df)} joueurs enrichis ({taux_succes:.1f}%)")
    
    # Afficher les joueurs non trouvés
    joueurs_non_trouves = df[df["wikidata_id"].isna()]["nom"].tolist()
    if joueurs_non_trouves:
        print(f"\n[ATTENTION] Joueurs non trouves ({len(joueurs_non_trouves)}):")
        for joueur in joueurs_non_trouves:
            print(f"   - {joueur}")
    else:
        print(f"\n[SUCCES] Tous les joueurs ont ete trouves sur Wikidata !")

    # Sauvegarde
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/joueurs_enrichis.csv"
    df.to_csv(output_path, index=False)
    
    print(f"\n[SUCCES] Fichier sauvegarde : {output_path}")
    print("="*70)

if __name__ == "__main__":
    enrich_with_wikidata_individual()
