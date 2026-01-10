import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import time
import math

def enrich_with_wikidata_smart_batch():
    print("> Démarrage de l'enrichissement (Méthode par paquets de 5)...")

    input_path = "data/raw/joueurs_base.csv"
    if not os.path.exists(input_path):
        print("❌ Erreur : Fichier d'entrée introuvable.")
        return

    df = pd.read_csv(input_path)
    noms_joueurs = df['nom'].dropna().unique().tolist()
    print(f"{len(noms_joueurs)} joueurs à traiter.")

    # Configuration Wikidata
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.addCustomHttpHeader("User-Agent", "Projet-Etudiant-Polytech/1.0")
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(60)

    cache_wikidata = {} # Dictionnaire pour stocker les résultats

    package_size = 5  # On traite par groupe de 5 joueurs
    total_packages = math.ceil(len(noms_joueurs) / package_size)

    for i in range(0, len(noms_joueurs), package_size):
        batch = noms_joueurs[i : i + package_size]
        print(f"   🔄 Traitement du paquet {i//package_size + 1}/{total_packages} ({len(batch)} joueurs)...")

        # Requête SPARQL pour chaque package
        noms_formatted = ' '.join([f'"{n}"@fr' for n in batch])
        
        query = f"""
        SELECT DISTINCT ?searchName ?item ?height ?birthPlaceLabel
        WHERE {{
          VALUES ?searchName {{ {noms_formatted} }}
          ?item rdfs:label ?searchName .
          ?item wdt:P31 wd:Q5 .
          ?item wdt:P106 wd:Q937857 .
          OPTIONAL {{ ?item wdt:P2048 ?height . }}
          OPTIONAL {{ ?item wdt:P19 ?birthPlace . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr". }}
        }}
        """
        sparql.setQuery(query)

        try:
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]
            
            for res in bindings:
                nom = res["searchName"]["value"]
                # Extraction sécurisée
                w_id = res["item"]["value"].split("/")[-1]
                raw_taille = res.get("height", {}).get("value", None)
                ville = res.get("birthPlaceLabel", {}).get("value", None)

                taille_en_m = None
                if raw_taille:
                    try:
                        val = float(raw_taille)
                        # Logique : Si > 3, c'est des cm (ex: 185), on convertit en m (1.85)
                        if val > 3:
                            taille_en_m = val / 100
                        else:
                            taille_en_m = val
                        taille_en_m = round(taille_en_m, 2) # Arrondir proprement à 2 chiffres après la virugle
                    
                    except ValueError:
                        taille_en_m = None
                
                # On ne garde que le premier résultat trouvé pour chaque nom
                if nom not in cache_wikidata:
                    cache_wikidata[nom] = {
                        "wikidata_id": w_id,
                        "taille_m": taille_en_m,
                        "ville_naissance": ville
                    }
            
            # Petite pause pour être gentil avec le serveur
            time.sleep(1)

        except Exception as e:
            print(f"⚠️ Erreur sur ce paquet : {e}")
            continue

    # --- FUSION ET SAUVEGARDE ---
    print("\n✅ Tous les paquets traités. Fusion des données...")
    
    df["wikidata_id"] = None
    df["taille_m"] = None
    df["ville_naissance"] = None

    for index, row in df.iterrows():
        nom = row['nom']
        if nom in cache_wikidata:
            info = cache_wikidata[nom]
            df.at[index, "wikidata_id"] = info["wikidata_id"]
            df.at[index, "taille_m"] = info["taille_m"]
            df.at[index, "ville_naissance"] = info["ville_naissance"]

    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/joueurs_enrichis.csv"
    df.to_csv(output_path, index=False)
    
    print(f"✅ SUCCÈS ! Fichier sauvegardé : {output_path}")

if __name__ == "__main__":
    enrich_with_wikidata_smart_batch()