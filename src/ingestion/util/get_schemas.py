import json
import pandas as pd

def generate_schema(df: pd.DataFrame, output_path: str = "data/schema.json") -> None:
    """
    Génère un fichier de métadonnées au format JSON (Table Schema) décrivant le DataFrame.
    
    Cette fonction décrit les types 
    et le sens de chaque colonne (Wikidata ID, Nom, Date, etc.).

    Args:
        df (pd.DataFrame): Le DataFrame contenant les données des joueurs nettoyées.
                           Il doit contenir au minimum les colonnes 'wikidata_id', 'nom', 
                           'date_naissance', 'poste'.
        output_path (str, optional): Le chemin relatif où sauvegarder le fichier JSON. 
                                     Par défaut "data/schema.json".

    Returns:
        None: La fonction ne retourne rien, elle crée un fichier sur le disque.

    Raises:
        IOError: Si le chemin de sortie n'est pas accessible.

    Exemple:
        >>> df = get_france_players()
        >>> generate_schema(df, "data/processed/schema_v1.json")
        > Fichier 'data/processed/schema_v1.json' généré avec succès.
    """
    print("Génération du schéma JSON ...")
    
    schema = {
        "title": "Joueurs de l'Equipe de France de Football",
        "description": "Liste consolidée des joueurs ayant joué en équipe de France masculine, identifiés via Wikidata.",
        "homepage": "https://github.com/yr16000/Mini-Projet-Ingenierie-des-donnees",
        "version": "1.0.0",
        "licence": "CC0-1.0",
        "resources": [
            {
                "name": "joueurs_edf",
                "path": "data/processed/joueurs_complet.csv",
                "format": "csv",
                "schema": {
                    "fields": [
                        {
                            "name": "wikidata_id",
                            "type": "string",
                            "description": "Identifiant unique du joueur sur Wikidata (ex: Q1065406)."
                        },
                        {
                            "name": "nom",
                            "type": "string",
                            "description": "Nom complet du joueur tel qu'enregistré sur Wikidata."
                        },
                        {
                            "name": "date_naissance",
                            "type": "date",
                            "format": "%Y-%m-%d",
                            "description": "Date de naissance au format ISO 8601."
                        },
                        {
                            "name": "poste",
                            "type": "string",
                            "description": "Position principale du joueur sur le terrain."
                        },
                        {
                            "name": "ville_naissance",
                            "type": "string",
                            "description": "Lieu de naissance."
                        }
                        # TODO : Ajouter les autres colonnes ici plus tard (Vitesse Valeur, etc.)
                    ]
                }
            }
        ]
    }

    # Sauvegarde du fichier JSON
    with open("data/schema.json", "w", encoding='utf-8') as f:
        json.dump(schema, f, indent=4, ensure_ascii=False)
    
    print("> Fichier 'data/schema.json' généré avec succès.")