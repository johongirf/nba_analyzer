from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from io import StringIO


CURRENT_YEAR = 2025



def collect_basic_player_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/basic_player_stats.csv'):
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
        html = requests.get(url)
        soup = BeautifulSoup(html.content, 'html.parser')
        soup.find('tr', 'norank').decompose()
        player_table = soup.find(id='per_game_stats')
        stats_table = pd.read_html(StringIO(str(player_table)))[0]
        stats_table["Year"] = year
        dfs.append(stats_table)
        time.sleep(5)
    advanced_stats = pd.concat(dfs)
    advanced_stats.to_csv(output_file_location)


def collect_advanced_player_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/advancedplayerstats.csv'): 
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{str(year)}_advanced.html"
        html = requests.get(url)
        soup = BeautifulSoup(html.content, 'html.parser')
        soup.find('tr', 'norank').decompose()
        player_table = soup.find(id='advanced')
        stats_table = pd.read_html(StringIO(str(player_table)))[0]
        stats_table["Year"] = year
        dfs.append(stats_table)
        time.sleep(3)
    advanced_stats = pd.concat(dfs)
    advanced_stats.to_csv(output_file_location)

def collect_team_data(start_year=CURRENT_YEAR - 1, end_year = CURRENT_YEAR, output_file_location='./nba_analyzer/data/NEW_TEAM_DATA_GOOD.csv'):
    dfs = []
    for year in range(start_year, end_year + 1):
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        html = requests.get(url) 

        
        soup = BeautifulSoup(html.content, 'html.parser')
        soup.find('tr', class_="thead").decompose()
        team_table = soup.find(id="divs_standings_E")
        team = pd.read_html(StringIO(str(team_table)))[0]
        team["Year"] = year
        team["Team"] = team["Eastern Conference"].str.replace("*", "", regex=False)
        del team["Eastern Conference"]
        dfs.append(team)
        
        soup = BeautifulSoup(html.content, 'html.parser')
        soup.find('tr', class_="thead").decompose()
        team_table = soup.find(id="divs_standings_W")
        team = pd.read_html(StringIO(str(team_table)))[0]
        team["Year"] = year
        team["Team"] = team["Western Conference"].str.replace("*", "", regex=False)
        del team["Western Conference"]
        dfs.append(team)
        time.sleep(3)

    team_stats = pd.concat(dfs)
    team_stats = team_stats[~team_stats["W"].str.contains("Division")]
    team_stats.to_csv(output_file_location)






if __name__ == "__main__":
    team_stats = pd.read_csv("./nba_analyzer/data/NEW_TEAM_DATA_GOOD.csv")
    team_stats = team_stats[~team_stats["W"].str.contains("Division")]
    team_stats.to_csv("./nba_analyzer/data/team_data.csv")



















