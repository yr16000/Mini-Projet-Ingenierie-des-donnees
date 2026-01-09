import pandas as pd
import requests
import os
import re
from io import StringIO

def get_current_squad_wikipedia():
    print("⚽ Récupération Effectif (Filtre Anti-Lignes Fusionnées)...")
    
    url = "https://fr.wikipedia.org/wiki/%C3%89quipe_de_France_de_football"
    headers = { "User-Agent": "Projet-Etudiant-Polytech/1.0" }
    
    try:
        response = requests.get(url, headers=headers)
        html_content = StringIO(response.text)
        
        # 1. Lecture brute (header=None pour tout attraper)
        dfs = pd.read_html(html_content, attrs={"class": "toccolours"}, header=None)
        
        if not dfs:
            print("❌ Aucun tableau trouvé.")
            return pd.DataFrame()

        df_brut = dfs[0]
        
        # 2. SCANNER INTELLIGENT
        header_index = -1
        
        # On scanne les 10 premières lignes
        for i in range(10):
            row = df_brut.iloc[i]
            
            # CRITÈRE 1 : Le texte "nom" et "club" doit être présent
            row_text = " ".join(row.astype(str).values).lower()
            has_keywords = "nom" in row_text and "club" in row_text
            
            # CRITÈRE 2 (LE SAUVEUR) : La ligne doit avoir au moins 3 cellules non-vides
            # Cela élimine les lignes fusionnées qui mettent tout le texte dans la colonne 0
            non_empty_cells = row.count() # Compte les valeurs qui ne sont pas NaN
            
            if has_keywords and non_empty_cells >= 4:
                print(f"✅ Vraie ligne d'entête trouvée à l'index {i} (avec {non_empty_cells} colonnes valides)")
                header_index = i
                break
        
        if header_index == -1:
            print("❌ Impossible de trouver une ligne d'entête valide (colonnes séparées).")
            # Debug : on affiche les lignes pour comprendre
            print(df_brut.head(5))
            return pd.DataFrame()

        # Application de l'entête
        df_brut.columns = df_brut.iloc[header_index]
        df = df_brut[header_index + 1:].copy()
        
        # Nettoyage des noms de colonnes
        df.columns = [str(c).strip() for c in df.columns]

        # 3. MAPPING DYNAMIQUE
        new_columns = {}
        for col in df.columns:
            col_clean = str(col).lower().strip()
            
            if "nom" in col_clean or "joueur" in col_clean:
                new_columns[col] = "nom"
            elif "naissance" in col_clean:
                new_columns[col] = "date_naissance"
            elif "club" in col_clean:
                new_columns[col] = "club"
            elif "n°" in col_clean or "num" in col_clean:
                new_columns[col] = "numero"

        df = df.rename(columns=new_columns)

        # Vérification
        required = ["nom", "date_naissance", "club"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"❌ ERREUR : Colonnes manquantes : {missing}")
            print(f"Colonnes actuelles : {list(df.columns)}")
            return pd.DataFrame()

        # 4. FILTRAGE ET NETTOYAGE
        
        # Filtre sur le numéro (garde seulement les joueurs, vire les titres "Attaquants")
        if 'numero' in df.columns:
            df = df[pd.to_numeric(df['numero'], errors='coerce').notnull()]
            df['numero'] = df['numero'].astype(float).astype(int)

        def clean_name(val):
            if pd.isna(val): return val
            val = str(val)
            val = re.sub(r"\[.*?\]", "", val)
            val = val.replace("(cap.)", "")
            return val.replace("\u00a0", " ").strip()

        def clean_date(val):
            if pd.isna(val): return val
            return str(val).split("(")[0].strip()

        df['nom'] = df['nom'].apply(clean_name)
        df['date_naissance'] = df['date_naissance'].apply(clean_date)
        
        df['wikidata_id'] = None
        
        # Réorganisation propre
        cols_final = ['numero', 'nom', 'date_naissance', 'club', 'wikidata_id']
        # On ne garde que les colonnes qui existent
        cols_final = [c for c in cols_final if c in df.columns]
        df = df[cols_final]

        return df

    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

if __name__ == "__main__":
    os.makedirs(os.path.join("data", "raw"), exist_ok=True)
    df = get_current_squad_wikipedia()
    
    if not df.empty:
        print(f"✅ SUCCÈS ! {len(df)} joueurs récupérés.")
        print(df.head())
        
        path = os.path.join("data", "raw", "joueurs_base.csv")
        df.to_csv(path, index=False)
        print(f"💾 Sauvegardé : {path}")
    else:
        print("⚠️ Toujours vide.")