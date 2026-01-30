import requests
import pandas as pd
import os
import difflib
from unidecode import unidecode

def get_sofifa_data():
    print("Récupération des stats via SoFIFA...")

    # 1. Chargement de tes données actuelles
    input_path = "data/processed/joueurs_enrichis.csv"
    if not os.path.exists(input_path):
        print("❌ Fichier joueurs_enrichis.csv introuvable.")
        return
    
    df = pd.read_csv(input_path)
    
    # 2. Configuration API SoFIFA
    # ID 1335 = Équipe de France
    url = "https://api.sofifa.net/team/1335" 
    
    headers = {
        "User-Agent": "Projet-Etudiant-Polytech/1.0",
        "Accept": "application/json"
    }

    try:
        print("🌍 Interrogation de l'API SoFIFA pour l'équipe de France (ID 1335)...")
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 429:
            print("❌ Erreur 429 : Trop de requêtes. Attends 1 minute.")
            return
        elif response.status_code != 200:
            print(f"❌ Erreur API : {response.status_code}")
            return

        data = response.json()
        
        # L'API renvoie un objet 'data' qui contient une liste 'players'
        # Regarde la structure JSON que tu m'as envoyée : { "data": { ..., "players": [...] } }
        squad_sofifa = data.get("data", {}).get("players", [])
        
        print(f"✅ SoFIFA a renvoyé {len(squad_sofifa)} joueurs dans l'effectif actuel.")

    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")
        return

    # 3. Préparation des données SoFIFA pour le matching
    # On crée une liste simplifiée pour faciliter la recherche
    sofifa_db = []
    for p in squad_sofifa:
        # On construit un nom complet pour comparer
        full_name = f"{p.get('firstName', '')} {p.get('lastName', '')}".strip()
        common_name = p.get('commonName', '')
        
        sofifa_db.append({
            "sofifa_id": p.get("id"),
            "full_name_clean": unidecode(full_name).lower(), # ex: "kylian mbappe"
            "common_name_clean": unidecode(common_name).lower(), # ex: "mbappe"
            "overall": p.get("overallRating"),
            "potential": p.get("potential"),
            "value_eur": p.get("value"), # Attention, parfois 0 pour les sélections nationales
            "wage_eur": p.get("wage"),
            "positions": p.get("position") # C'est un code numérique (ex: 28), faudra mapper si besoin
        })

    # 4. Fonction de Matching (Le Cœur du script)
    def find_player_stats(nom_csv):
        nom_csv_clean = unidecode(str(nom_csv)).lower()
        
        # Etape A : Recherche Exacte sur le nom commun (ex: "Mbappé" vs "Mbappé")
        for p in sofifa_db:
            if p["common_name_clean"] in nom_csv_clean or nom_csv_clean in p["common_name_clean"]:
                return p
            if p["full_name_clean"] == nom_csv_clean:
                return p
        
        # Etape B : Recherche "Floue" (Si petite faute de frappe)
        # On récupère tous les noms complets SoFIFA
        all_names = [p["full_name_clean"] for p in sofifa_db]
        # On demande à Python : "Quel est le nom le plus proche ?"
        matches = difflib.get_close_matches(nom_csv_clean, all_names, n=1, cutoff=0.6)
        
        if matches:
            best_match_name = matches[0]
            # On retrouve le joueur associé à ce nom
            for p in sofifa_db:
                if p["full_name_clean"] == best_match_name:
                    return p
                    
        return None

    # 5. Application sur ton DataFrame
    print("🔄 Fusion des données (Matching des noms)...")
    
    sofifa_stats = []
    
    for nom in df['nom']:
        stats = find_player_stats(nom)
        if stats:
            sofifa_stats.append(stats)
        else:
            # Si pas trouvé, on met des vides
            sofifa_stats.append({
                "sofifa_id": None, "overall": None, "potential": None, 
                "value_eur": None, "wage_eur": None
            })

    # Conversion en DataFrame pour concaténer proprement
    df_stats = pd.DataFrame(sofifa_stats)
    
    # On ajoute les colonnes au DataFrame principal
    df['sofifa_overall'] = df_stats['overall']
    df['sofifa_potential'] = df_stats['potential']
    df['sofifa_wage'] = df_stats['wage_eur']
    
    # 6. Sauvegarde
    output_path = "data/processed/joueurs_complets.csv"
    df.to_csv(output_path, index=False)
    
    # Aperçu
    print(f"💾 Sauvegardé : {output_path}")
    print(df[['nom', 'sofifa_overall', 'sofifa_potential']].head(10))

if __name__ == "__main__":
    # Petit check pour installer unidecode si tu ne l'as pas
    try:
        import unidecode
        get_sofifa_data()
    except ImportError:
        print("⚠️ Il manque une librairie. Lance cette commande :")
        print("pip install unidecode")