import pandas as pd
# Nécessite: pip install soccerdata
import soccerdata as sd 
import logging

# On désactive les logs bavards de soccerdata
logging.getLogger("soccerdata").setLevel(logging.WARNING)

# Initialisation globale (pour ne pas le refaire à chaque joueur)
# On cible les 5 grands championnats ou juste la Ligue 1 selon besoin
try:
    tm = sd.Transfermarkt(leagues="FRA-Ligue 1", seasons=2023)
    print("   [Init] Transfermarkt chargé.")
except Exception as e:
    print(f"   [Init] Erreur chargement Transfermarkt: {e}")
    tm = None

def get_transfermarkt_real_value(nom_joueur):
    """
    Récupère la valeur marchande via soccerdata.
    """
    if tm is None:
        return {}

    try:
        # TODO: Utiliser tm.read_player_valuations() ou tm.read_players()
        # Il faut filtrer le dataframe pour trouver le 'nom_joueur'
        
        # Exemple théorique de logique :
        # df = tm.read_players()
        # joueur = df[df.index.str.contains(nom_joueur, case=False, na=False)]
        
        # S'ils ne trouvent rien :
        # return {}

        # S'ils trouvent, ils renvoient :
        return {
            "valeur_marchande_euro": 0, # Mettre la vraie valeur (int)
            "club_actuel": "Inconnu"    # Mettre le vrai club (str)
        }

    except Exception as e:
        print(f"   [TM] Erreur pour {nom_joueur}: {e}")
        return {}