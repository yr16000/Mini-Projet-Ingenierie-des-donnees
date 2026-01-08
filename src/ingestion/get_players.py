import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import os

def get_france_players():
    print("Récupération des joueurs depuis Wikidata...")
    
    # Se connecter au endpoint public de Wikidata
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    
    # REQUÊTE SPARQL :
    # Retourne ID, Nom, Date de naissance
    # Condition : A joué pour (P54) l'équipe de France masculine (Q47774)
    query = """
    SELECT DISTINCT ?player ?playerLabel ?dob ?positionLabel
    WHERE {
      ?player wdt:P54 wd:Q47774 .       # A joué pour l'équipe de France
      ?player wdt:P569 ?dob .            # A une date de naissance
      OPTIONAL { ?player wdt:P413 ?position . } # Optionnel: A un poste
      
      SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
    }
    ORDER BY ?playerLabel
    """
    
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # Transformation des résultats en liste propre
    players_data = []
    for result in results["results"]["bindings"]:
        
        # Garder juste l'ID (ex: Q12345)
        wikidata_id = result["player"]["value"].split("/")[-1]
        
        # Nettoyage de la date (-> YYYY-MM-DD)
        raw_date = result["dob"]["value"]
        clean_date = raw_date.split("T")[0] 
        
        player_info = {
            "wikidata_id": wikidata_id,
            "nom": result["playerLabel"]["value"],
            "date_naissance": clean_date,
            "poste": result.get("positionLabel", {}).get("value", "Inconnu")
        }
        players_data.append(player_info)


    df = pd.read_json(pd.io.json.json_normalize(players_data).to_json())    
    # On supprime les doublons (certains joueurs ont plusieurs postes enregistrés)
    df = df.drop_duplicates(subset=['wikidata_id'])
    
    return df

if __name__ == "__main__":
    # Création du dossier de sortie si inexistant
    os.makedirs("data/raw", exist_ok=True)
    
    df_players = get_france_players()
    
    print(f"Succès ! {len(df_players)} joueurs récupérés.")
    print(df_players.head())
    
    # Save en CSV
    df_players.to_csv("data/raw/joueurs_base.csv", index=False)
    print("Fichier sauvegardé dans 'data/raw/joueurs_base.csv'")