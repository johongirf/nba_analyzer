from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from io import StringIO


CURRENT_YEAR = 2025
NICKNAMES = {}

with open("./nba_analyzer/data/nicknames.csv") as f:
    lines = f.readlines()
    for line in lines:
        name, abbrev = line.replace("\n", "").split(",")
        NICKNAMES[abbrev] = name


def collect_basic_player_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/basic_player_stats.csv'):
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
        html = requests.get(url)
        soup = BeautifulSoup(html.content, "html.parser")
        soup.find("tr", "norank").decompose()
        player_table = soup.find(id="per_game_stats")
        stats_table = pd.read_html(StringIO(player_table))[0]
        stats_table["Year"] = year
        dfs.append(stats_table)
        time.sleep(5)
    player_stats = pd.concat(dfs)
    player_stats = player_stats.drop(["Rk"], axis=1)
    player_stats = player_stats.groupby(["Player", "Year"]).apply(combine_player_teams)
    player_stats.index = player_stats.index.droplevel()
    player_stats.index = player_stats.index.droplevel()
    player_stats.to_csv(output_file_location, index=False)


def collect_advanced_player_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/advanced_player_stats.csv'): 
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{str(year)}_advanced.html"
        html = requests.get(url)
        soup = BeautifulSoup(html.content, "html.parser")
        soup.find("tr", "norank").decompose()
        player_table = soup.find(id="advanced")
        stats_table = pd.read_html(StringIO(player_table))[0]
        stats_table["Year"] = year
        dfs.append(stats_table)
        time.sleep(3)
    advanced_stats = pd.concat(dfs)
    advanced_stats = advanced_stats.drop(["Rk"], axis=1)
    advanced_stats = advanced_stats.groupby(["Player", "Year"]).apply(combine_player_teams)
    advanced_stats.index = advanced_stats.index.droplevel()
    advanced_stats.index = advanced_stats.index.droplevel()
    advanced_stats.to_csv(output_file_location, index=False)

def collect_team_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/team_stats.csv'):
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        html = requests.get(url) 

        
        soup = BeautifulSoup(html.content, "html.parser")
        soup.find("tr", class_="thead").decompose()
        team_table = soup.find(id="divs_standings_E")
        team = pd.read_html(StringIO(team_table))[0]
        team["Year"] = year
        team["Team"] = team["Eastern Conference"].str.replace("*", "", regex=False)
        del team["Eastern Conference"]
        dfs.append(team)
        
        soup = BeautifulSoup(html.content, 'html.parser')
        soup.find('tr', class_="thead").decompose()
        team_table = soup.find(id="divs_standings_W")
        team = pd.read_html(StringIO(team_table))[0]
        team["Year"] = year
        team["Team"] = team["Western Conference"].str.replace("*", "", regex=False)
        del team["Western Conference"]
        dfs.append(team)
        time.sleep(3)

    team_stats = pd.concat(dfs)
    team_stats = team_stats.drop(["Unnamed: 0"], axis=1)
    team_stats = team_stats[~team_stats["W"].str.contains("Division")]
    team_stats["GB"] = team_stats["GB"].replace("—", "0")
    team_stats.to_csv(output_file_location, index=False)

def combine_player_teams(df):
    if df.shape[0] == 1:
        return df
    row = df[df["Team"].str.endswith("TM")]
    row["Team"] = df.loc[df.index[-1],"Team"]
    return row
    
    
def combine_player_team_stats(basic_df: pd.DataFrame, advanced_df: pd.DataFrame, team_df: pd.DataFrame):
    basic_df = basic_df[
        ["Player","Age", "Team", "Pos", "G", "GS", "MP", "FG", "FGA", "FG%", 
        "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", 
        "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "Awards", "Year"]]
    advanced_df = advanced_df[
        ["Player", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", 
        "AST%", "STL%", "BLK%", "TOV%", "USG%", "OWS", 
        "DWS", "WS", "WS/48", "OBPM", "DBPM", "BPM", 
        "VORP", "Year"]]
    combined = basic_df.merge(advanced_df, how="outer", on=["Player", "Year"])
    combined["Tm"] = combined["Team"].copy()
    combined["Team"] = combined["Team"].map(NICKNAMES)
    combined = combined.merge(team_df, how="outer", on=["Team", "Year"])

    combined.to_csv("./nba_analyzer/data/combined_data.csv")



if __name__ == "__main__":
    collect_advanced_player_data(2000, 2025)
    collect_basic_player_data(2000, 2025)
    collect_team_data(2000, 2025)
    basic_data = pd.read_csv("./nba_analyzer/data/basic_player_stats.csv")
    advanced_data = pd.read_csv("./nba_analyzer/data/advanced_player_stats.csv")
    team_data = pd.read_csv("./nba_analyzer/data/team_stats.csv")
    combine_player_team_stats(basic_data, advanced_data, team_data)
    
    
        
















